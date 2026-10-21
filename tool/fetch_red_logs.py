#!/usr/bin/env python3
# Parts of this file follow TestGrid's cmd/updater, pkg/updater, util/gcs, util/queue, config, config/snapshot,
# cmd/config_merger, pkg/merger, pkg/tabulator and cmd/tabulator (f15bfa4808a4; Copyright The Kubernetes Authors and
# The TestGrid Authors), the GCS client's reading of a gzip-encoded object (cloud.google.com/go/storage v1.31.0;
# Copyright Google LLC) and go-retry's retries (github.com/sethvargo/go-retry v0.2.4), Apache License 2.0
# (LICENSE), Go's net/url, strings, path and encoding/json (1.26.5; Copyright The Go Authors), BSD 3-Clause
# license in third_party/go/LICENSE, and Unicode's White_Space characters (15.0.0; Copyright Unicode, Inc.),
# Unicode License in third_party/unicode/LICENSE. THIRD_PARTY_NOTICES.md lists what was ported.
"""Archive the raw Prow build-log.txt behind the red cells on TestGrid dashboards.

A cell counts as red exactly when TestGrid's own UI paints it red (#a00):
FAIL, TIMED_OUT, CATEGORIZED_FAIL and TOOL_FAIL. Tables are read the way the
dashboard page loads them (tab base options applied, no extra row filter), so
the red cells are the ones you see when you open the tab. Each column is one
Prow build: the archive keeps one copy per build and skips builds it already
holds, so each run takes the red builds TestGrid shows that the archive does
not hold yet (a cell red only between two runs, on a build still shown, is not
seen), and the first run backfills everything TestGrid still shows. Builds that
could not be archived yet are remembered in state.json and retried straight
from GCS, even after they scroll off TestGrid.

When a tab's history no longer reaches back to the previous scan (a missed run,
or a tab that keeps little history), its builds from that time are listed in
GCS and judged by TestGrid's own rule (testgrid_rule.py, a port of its updater
checked against TestGrid's code): those it painted red on the tab, or may have
where a read is in doubt, are archived; those it would still read again (not
finished), or that the tab may have shown under a config the archive does not
know yet or does not follow, are watched until they can be judged, at most 14
days after they were made, or 60 under such a config.

A matching md5 only proves the copy equals what GCS held at that moment, and
Prow can upload a log again (its sidecar uploads once when told to stop and
again when the test exits). So every archived build is compared with GCS on
every run until RECHECK_HOURS after it finished (GIVE_UP_DAYS after it was
archived, for one without a log or finished.json), and fetched again whenever
GCS holds different content. After that (it is final), it is compared at least
once a week: each run compares the seventh of the archive for its UTC day, and
those of the days since the last run done with its own (it compared them all,
and its verification found none differing: see gcs_sevenths), as
cross_check.py does. So one a run cut off did not get to is compared by the
next run. A comparison that fails is an error, and is made again by the next
run; once a build's comparisons have failed for GIVE_UP_DAYS, the runs give up
on it: it is still compared at least once a week, but a failure on GCS's side
is no error, and does not keep a run from being done with its sevenths (see
COMPARISON_FAILING). A copy taken less than SETTLE_HOURS after the build
finished, or before it finished at all, is marked as not settled.

Each run gets its own folder; a build is stored in the folder of the run that
first archived it, and later repairs or refreshes happen in place. Layout
under --archive:
  runs/<YYYY-MM>/<run>/             one run, named by its UTC start time
    run.json                        what the run scanned, archived and failed
    README.md                       the builds it archived, with links
    verification.json               its verification: what was checked, and the outcome (cross_check.py)
    <job>/<build>/build-log.txt     raw log, length and md5 checked against GCS
                                    (build-log.txt.gz with --gzip; meta.json keeps the raw md5)
    <job>/<build>/podinfo.json      Prow's record of the build's pod and node, when GCS has one
                                    (podinfo.json.gz with --gzip)
    <job>/<build>/<file>.000, ...   a file larger than --max-file-mb, in numbered parts
    <job>/<build>/meta.json         tabs, red cells, TestGrid messages, result, GCS identity
  charts/*.svg                      trend charts shown in INDEX.md
  INDEX.md                          charts, builds still missing, recent runs and builds
  weeks/<ISO week>.md, .json        every archived build, one page per ISO week (UTC)
  index.json                        the list of weekly pages
  state.json                        per-tab last scan, builds to retry or given up on
  excluded.json                     builds taken out for good, by hand (see load_excluded)
"""
import argparse
import bisect
import base64
import concurrent.futures as cf
import contextlib
import copy
import datetime as dt
import email.utils
import fcntl
import functools
import gzip
import hashlib
import http.client
import io
import itertools
import json
import math
import operator
import os
import random
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

import urllib3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402

TESTGRID = "https://testgrid.k8s.io"
GCS = "https://storage.googleapis.com"
DASHBOARDS = tgc.DASHBOARDS  # (TestGrid's config is built for these: one list)
# TestGrid TestStatus values its UI paints red (#a00). BUILD_FAIL (11) is
# painted black and UNKNOWN (6) grey, so those cells are not red.
RED = {9: "TIMED_OUT", 10: "CATEGORIZED_FAIL", 12: "FAIL", 14: "TOOL_FAIL"}
NO_RESULT = 0
STATUSES = range(16)  # TestGrid's TestStatus values, NO_RESULT (0) to BUILD_PASSED (15)
# Prow build IDs are snowflakes: (id >> 22) + this epoch is the creation time in ms.
PROW_EPOCH_MS = 1288834974657
JOB_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
BUILD_RE = re.compile(r"[0-9]{1,20}")
# <bucket>/<path>/<job>: plain segments only, so it cannot step out of the bucket in a URL.
QUERY_RE = re.compile(r"[a-z0-9][a-z0-9._-]*(?:/(?!\.\.?(?:/|$))[A-Za-z0-9._-]+)+")
LOG_FILES = {"build-log.txt", "build-log.txt.gz", "podinfo.json", "podinfo.json.gz"}
# a stored file too large for one file in git is kept as numbered parts: <file>.000, <file>.001, ...
PART_RE = re.compile(r"(build-log\.txt(?:\.gz)?|podinfo\.json(?:\.gz)?)\.([0-9]{3})")
RUN_FILES = {"run.json", "README.md", "verification.json"}  # in a run folder, next to its job folders
# verification.json's outcomes (cross_check.py --record): "incomplete" until the verification ends
VERIFICATION_OUTCOMES = ("incomplete", "ok", "problems", "timed out", "interrupted", "stopped")
RUN_RE = re.compile(r"([0-9]{4}-[0-9]{2})-[0-9]{2}T[0-9]{6}Z(?:-([1-9][0-9]*))?")
RETRIES = 4
BACKOFF = 2.0  # seconds before the first retry, doubled after each attempt, with jitter
RETRY_AFTER_CAP = 60.0  # never wait longer than this for one Retry-After
# A response body must finish within BASE_DEADLINE plus its size at MIN_RATE,
# so a server that trickles bytes cannot stall a request (socket timeouts are
# per read); the whole run is bounded by --run-timeout-minutes on top of that.
BASE_DEADLINE = 120.0
MIN_RATE = 50_000  # bytes per second
MAX_JSON_BYTES = 256 << 20  # a TestGrid summary or table larger than this is refused
MAX_DOWNLOAD_BYTES = 4 << 30  # a GCS object larger than this, stored or gunzipped, is refused rather than streamed forever
MAX_LIST_PAGES = 1000  # a GCS listing that pages on longer than this is broken
GUNZIP_STEP = 256 << 10  # gunzip yields at most this many bytes per step, whatever the ratio
GIVE_UP_DAYS = 14  # give up on a build GCS still lacks, or one whose comparisons with GCS keep failing, after this long
VANISH_ROUNDS = 4  # how many times HoldModel works out again which builds vanish, as they change each other
MAX_LOOKBACK = 21 * 86400  # how far before a build its job's other builds are read, at most (see Timeline.most)
HELD_DAYS = 60  # watch a build only its tab's unfollowed config keeps undecided this long
# Prow's last upload for a build comes within about 20 minutes of its finished.json
# time, and GCS may keep serving a public object's old content from cache for up to
# 60 minutes after an overwrite (https://cloud.google.com/storage/docs/consistency),
# so only a copy taken or confirmed this long after the build finished is settled.
SETTLE_HOURS = 2
RECHECK_HOURS = 48  # compare an archived build with GCS on every run until this long after it finished
UNLISTED_DAYS = 30  # forget a tab missing from its dashboard's summary after this long
STOPPED = "the run stopped before it finished"  # the error of a run's report until the run ends (see run())
PENDING_KEPT = 60  # ranges of builds a scan saw that a tab keeps waiting for a config, at most (see run())
SERIOUS = ("may be missing", "state.json", "no longer listed", "old layout",  # warnings that fail the run
           "cannot be judged from GCS", "TestGrid's config was last read", "TestGrid's config was never read",
           "the filters cannot be applied here", "does not match the tab", "still not known",
           "lacks tabs TestGrid shows", "has not run since", "were not compared with GCS before the run")
CONFIG_STALE_DAYS = 3  # TestGrid's config not read for this long is a serious warning
CONFIG_KEEP_DAYS = HELD_DAYS + 17  # TestGrid's config over time is kept this long (a held build's days of results)
BACKFILL_SLACK_HOURS = 6  # a build may start this long after its ID was made
PROWJOB_WAIT = 30  # seconds to wait for prowjob.json; without it a tab keeps the margin it had
GIVEN_UP_KEEP_DAYS = 30  # a build given up on stays listed this long, then only the run reports name it
FUTURE_SLACK = 86400  # timestamps more than a day ahead are rejected as bogus


def _umask():
    mask = os.umask(0)
    os.umask(mask)
    return mask


UMASK = _umask()  # read once at import, before any worker threads exist


class NotFound(Exception):
    pass


class IntegrityError(Exception):
    pass


class Abandoned(Exception):
    pass


class LocalIOError(Exception):
    """A local disk error: not the server's fault, so never retried or treated as pushback."""


# Socket timeouts, resets and SSL errors are OSError; urllib3 wraps the rest.
TRANSIENT = (OSError, http.client.HTTPException, urllib3.exceptions.HTTPError, IntegrityError)
# What a bug in this code raises, not GCS (see COMPARISON_FAILING). One http.client raises as it parses a response's
# headers is the response's (see FinalResponse.begin and _read_status; begin takes one of _read_status's own code for
# that too).
BUGS = (TypeError, AttributeError, NameError, LookupError, AssertionError, ZeroDivisionError, RecursionError,
        NotImplementedError)


def cpu_count():
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


@functools.cache
def tool_version():
    """The code that runs: {"commit": the commit checked out (git rev-parse HEAD), "modified":
    whether tool/ differs from it (git status)}, each None when git cannot tell."""
    def git(*args):  # (its output as bytes: a file name need not be UTF-8)
        try:
            done = subprocess.run(["git", "-C", os.path.dirname(os.path.abspath(__file__)), *args],
                                  capture_output=True, timeout=60)
        except (OSError, subprocess.SubprocessError):
            return None
        return done.stdout if done.returncode == 0 else None
    commit, status = git("rev-parse", "HEAD"), git("--no-optional-locks", "status", "--porcelain", "--untracked-files=normal",
                                                    "--", ".")
    return {"commit": (commit or b"").decode("ascii", "replace").strip() or None,
            "modified": None if status is None else bool(status.strip())}


def github_run():
    """The GitHub Actions run this runs in ({"repository", "run_id", "run_attempt"}), None outside one."""
    if not os.environ.get("GITHUB_RUN_ID"):
        return None
    return {k: os.environ.get(f"GITHUB_{k.upper()}") for k in ("repository", "run_id", "run_attempt")}


class AdaptiveLimit:
    """Caps in-flight HTTP requests to one origin and retunes the cap while running (AIMD).

    The work is network-bound, so speed comes from how many requests are in
    flight; the CPU-heavy parts (TLS, gzip, md5) run in C with the GIL
    released, so the worker threads already spread them over the cores.
    The cap grows by one after a full cap's worth of successes and halves when
    servers push back (429, 503, 504, timeouts, resets) - at most once a
    second, so one burst of failures counts as one signal.
    """

    def __init__(self, start, ceiling):
        self.ceiling = max(1, ceiling)
        self.cap = float(max(1, min(start, self.ceiling)))
        self.peak = int(self.cap)
        self.pushbacks = 0
        self._active = 0
        self._streak = 0
        self._last_cut = -math.inf
        self._cond = threading.Condition()

    def acquire(self):
        with self._cond:
            while self._active >= int(self.cap):
                self._cond.wait()
            self._active += 1

    def release(self, pushback):
        with self._cond:
            self._active -= 1
            if pushback:
                self.pushbacks += 1
                now = time.monotonic()
                if now - self._last_cut >= 1.0:
                    self.cap = max(1.0, self.cap / 2)
                    self._last_cut = now
                self._streak = 0
            else:
                self._streak += 1
                if self._streak >= int(self.cap) and self.cap < self.ceiling:
                    self.cap = min(self.cap + 1, float(self.ceiling))
                    self._streak = 0
                    self.peak = max(self.peak, int(self.cap))
            self._cond.notify_all()


class RunContext:
    """What one run's threads share: a request limiter per origin, the abandon flag and the deadline.

    Each origin (TestGrid, GCS) gets its own AdaptiveLimit, starting like
    `limit`, so one server pushing back does not slow requests to the other.
    main() binds a fresh context to the main thread and to every worker of its
    pool, so a later run in the same process never re-arms or shares an earlier
    run's abandoned workers.
    """

    def __init__(self, limit, deadline=math.inf):
        self.limit = limit  # what each origin's limit starts as
        self.limits = {}  # origin -> AdaptiveLimit
        self._lock = threading.Lock()
        self.abandoned = threading.Event()
        self.write_lock = threading.RLock()  # held while checking `abandoned` and writing (see abandon())
        self.deadline = deadline  # time.monotonic() by which the run stops waiting
        self.folders = {}  # (gcs, query, build) -> (its folder, or None and why it cannot be read): see build_folder

    def abandon(self):
        """Stop the workers: once this returns, none is in the middle of changing a file of
        the archive (a rename or a removal) or can start to, as replace() and remove()
        check under the same lock; at most a temp file git ignores can still appear. A
        signal while it waits for the lock (a KeyboardInterrupt) is raised once they are
        stopped, not before."""
        interrupt = None
        while True:
            try:
                with self.write_lock:
                    self.abandoned.set()
                break
            except KeyboardInterrupt as e:
                interrupt = e
        if interrupt:
            raise interrupt

    def limit_for(self, url):
        origin = "://".join(urllib.parse.urlsplit(url)[:2])
        with self._lock:
            if origin not in self.limits:
                self.limits[origin] = AdaptiveLimit(int(self.limit.cap), self.limit.ceiling)
            return self.limits[origin]

    def stats(self):
        """Per origin, and over all of them: the peak and final caps and the pushbacks."""
        with self._lock:
            per = {o: {"peak": lim.peak, "final": int(lim.cap), "pushbacks": lim.pushbacks}
                   for o, lim in sorted(self.limits.items())}
        return {"peak": max((p["peak"] for p in per.values()), default=int(self.limit.cap)),
                "final": min((p["final"] for p in per.values()), default=int(self.limit.cap)),
                "pushbacks": sum(p["pushbacks"] for p in per.values()), "origins": per}


_local = threading.local()
_DEFAULT_CONTEXT = RunContext(AdaptiveLimit(8, 8))
_last_run = None  # the context of the run main() last started (see __main__: main() binds the caller's again)


def context():
    return getattr(_local, "context", _DEFAULT_CONTEXT)


def bind_context(ctx):
    _local.context = ctx


# Keep-alive connections, one pool per origin, shared by a run's threads: most
# requests are small HEADs and GETs to the same two hosts, and reusing a
# connection saves its TLS handshake (measured: 15-37% faster). request()
# does the retrying, and follows redirects itself (same host only).
MAX_INTERIM = 50  # interim (1xx) responses taken before a final one ...
INTERIM_SECONDS = 60.0  # ... and for at most this long (each is read with the request's timeout)


class FinalResponse(http.client.HTTPResponse):
    """http.client's response, past every interim (1xx) response before it, as an HTTP/1.1
    client must take them: http.client skips only 100 Continue, so a 102 or a 103 (Early
    Hints) would be taken for the response, and the real one left on the connection for
    the next request. 101 (Switching Protocols) is final, and refused by fetch_once."""

    def begin(self):
        try:
            super().begin()
        except BUGS as e:  # (http.client's, as it parses headers it cannot read: an odd Content-Type)
            raise IntegrityError(f"a response that cannot be read ({type(e).__name__}: {e})") from None

    def _read_status(self):
        start = time.monotonic()
        for _ in range(MAX_INTERIM + 1):
            version, status, reason = super()._read_status()
            if not 100 <= status < 200 or status == 101:
                return version, status, reason
            if time.monotonic() - start > INTERIM_SECONDS:
                raise http.client.HTTPException(f"interim responses for more than {INTERIM_SECONDS:.0f} s")
            try:
                http.client.parse_headers(self.fp)  # its headers (bounded as any response's are)
            except BUGS as e:  # (as in begin: urllib3 reads a proxy's answer to CONNECT with this alone)
                raise IntegrityError(f"a response that cannot be read ({type(e).__name__}: {e})") from None
        raise http.client.HTTPException(f"more than {MAX_INTERIM} interim responses")


class _HTTPConnection(urllib3.connection.HTTPConnection):
    response_class = FinalResponse


class _HTTPSConnection(urllib3.connection.HTTPSConnection):
    response_class = FinalResponse


class _HTTPPool(urllib3.HTTPConnectionPool):
    ConnectionCls = _HTTPConnection


class _HTTPSPool(urllib3.HTTPSConnectionPool):
    ConnectionCls = _HTTPSConnection


POOL_CLASSES = {"http": _HTTPPool, "https": _HTTPSPool}
HTTP = urllib3.PoolManager(num_pools=16, maxsize=64, block=False, retries=False)
HTTP.pool_classes_by_scheme = POOL_CLASSES
_PROXIES, _PROXY_POOLS, _PROXY_LOCK = urllib.request.getproxies(), {}, threading.Lock()
MAX_REDIRECTS = 10


def pool_for(url):
    """The pool a request to url goes through: a proxy's if the environment names one for
    its scheme and does not exempt its host (http_proxy, https_proxy, no_proxy), else HTTP.
    As urllib takes them: a proxy without a scheme is an http:// one, and one with a user
    and password gets them as Proxy-Authorization."""
    parts = urllib.parse.urlsplit(url)
    proxy = _PROXIES.get(parts.scheme)
    if not proxy or urllib.request.proxy_bypass(parts.netloc):
        return HTTP
    with _PROXY_LOCK:
        if proxy not in _PROXY_POOLS:
            p = urllib.parse.urlsplit(proxy if "://" in proxy else "http://" + proxy)
            auth = (urllib.parse.unquote(p.username) + ":" + urllib.parse.unquote(p.password)
                    if p.username and p.password else None)
            _PROXY_POOLS[proxy] = urllib3.ProxyManager(
                p._replace(netloc=p.netloc.rsplit("@", 1)[-1]).geturl(), num_pools=16,
                maxsize=HTTP.connection_pool_kw["maxsize"], block=False, retries=False,
                proxy_headers=urllib3.make_headers(proxy_basic_auth=auth, proxy_basic_auth_encoding="utf-8")
                if auth else None)
            _PROXY_POOLS[proxy].pool_classes_by_scheme = POOL_CLASSES
        return _PROXY_POOLS[proxy]


class Headers:
    """A response's headers, read as from http.client: get() gives the first value, get_all() each one."""

    def __init__(self, raw):
        self.raw = raw

    def get(self, name, default=None):
        values = self.raw.getlist(name)
        return values[0] if values else default

    def get_all(self, name):
        return self.raw.getlist(name) or None


class Response:
    """A response as request() hands it to `consume`: its url, status, headers and raw body."""

    def __init__(self, raw, url):
        self.raw, self.url, self.status, self.headers = raw, url, raw.status, Headers(raw.headers)

    def read1(self, n=-1):
        return b"" if n == 0 else self.raw.read1(n if n and n > 0 else None)

    def read(self, n=-1):
        return b"" if n == 0 else self.raw.read(n if n and n > 0 else None)


def settle(raw, reuse):
    """Return a connection to its pool only when `reuse`: a final response whose body
    was read to the end (an empty one, as for a HEAD, is marked done here). Any other is
    closed, so leftover bytes are never taken for the next response and an error body
    cannot hold a request without a deadline."""
    if reuse and not raw.isclosed() and raw.length_remaining == 0:
        raw.drain_conn()
    if not raw.isclosed() or not reuse:
        raw.close()
    raw.release_conn()


def closed_on_reuse(e):
    """An error that means the server had closed the kept-alive connection the request went out on."""
    return isinstance(e, urllib3.exceptions.ProtocolError) and any(
        isinstance(a, (ConnectionResetError, BrokenPipeError)) for a in e.args)


def fetch_once(url, consume, headers, timeout, method):
    """One attempt at url, following same-host redirects: consume(response) for a
    2xx; any other status is an HTTPError, as urllib would raise it. A kept-alive
    connection the server has just closed is replaced at once, without counting."""
    for _ in range(MAX_REDIRECTS + 1):
        for again in (False, True):
            try:
                raw = pool_for(url).request(method, url, headers=headers or {}, preload_content=False,
                                            decode_content=False, redirect=False,
                                            timeout=urllib3.Timeout(connect=timeout, read=timeout))
                break
            except urllib3.exceptions.ProtocolError as e:
                if again or not closed_on_reuse(e):
                    raise
            except IntegrityError as e:  # (FinalResponse's: a response it cannot read)
                raise IntegrityError(f"{url}: {e}") from None
        consumed = False
        try:
            if raw.status < 200:  # (101: FinalResponse takes every other 1xx as interim)
                raise IntegrityError(f"{url}: unexpected informational response {raw.status}")
            if raw.status in (301, 302, 303, 307, 308) and raw.headers.getlist("Location"):
                if len(raw.headers.getlist("Location")) != 1:
                    raise urllib.error.HTTPError(url, raw.status, "more than one Location header",
                                                 Headers(raw.headers), io.BytesIO())
                new = urllib.parse.urljoin(url, raw.headers.get("Location"))
                old, to = urllib.parse.urlsplit(url), urllib.parse.urlsplit(new)
                if (old.scheme, old.netloc) != (to.scheme, to.netloc):
                    raise urllib.error.HTTPError(url, raw.status, f"refused redirect to another host ({to.netloc})",
                                                 Headers(raw.headers), io.BytesIO())
                url = new
                continue
            if not 200 <= raw.status < 300:
                raise urllib.error.HTTPError(url, raw.status, raw.reason or "", Headers(raw.headers), io.BytesIO())
            result = consume(Response(raw, url))
            consumed = True
            return result
        finally:
            settle(raw, reuse=consumed)
    raise urllib.error.HTTPError(url, 310, f"more than {MAX_REDIRECTS} redirects", None, io.BytesIO())


def deadline_for(r):
    try:
        size = header_int(r.headers, "Content-Length")
    except IntegrityError:
        size = None  # (reading the body says so)
    return time.monotonic() + BASE_DEADLINE + (size / MIN_RATE if size else 0)


def chunks(r, url):
    """Yield the body in small reads, failing once it falls behind its deadline."""
    end, abandoned = deadline_for(r), context().abandoned
    read = getattr(r, "read1", r.read)
    while True:
        try:
            chunk = read(64 << 10)
        except urllib3.exceptions.ProtocolError as e:
            if any(isinstance(a, http.client.IncompleteRead) for a in e.args):
                # cut off short of its Content-Length: retried, but not the server pushing back
                raise IntegrityError(f"{url}: body cut short ({e.args[-1]})") from None
            raise
        if not chunk:
            return
        yield chunk
        if time.monotonic() > end:
            raise IntegrityError(f"{url}: response too slow")
        if abandoned.is_set():
            raise Abandoned(url)


def header_int(headers, name):
    """A non-negative integer header, None when absent; anything else is an IntegrityError."""
    value = headers.get(name)
    if value is None:
        return None
    value = value.strip()
    if not value.isascii() or not value.isdigit() or len(value) > 19:
        raise IntegrityError(f"bad {name} header {value[:40]!r}")
    return int(value)


def read_body(r, limit=None):
    limit = MAX_JSON_BYTES if limit is None else limit
    size = header_int(r.headers, "Content-Length")
    if size is not None and size > limit:
        raise IntegrityError(f"{r.url}: {size} bytes is more than the {limit} byte limit")
    body, total = [], 0
    for chunk in chunks(r, r.url):
        total += len(chunk)
        if total > limit:
            raise IntegrityError(f"{r.url}: more than the {limit} byte limit")
        body.append(chunk)
    body = b"".join(body)
    # A body cut off short of its Content-Length (urllib3 raises for that too).
    if size is not None and size != len(body):
        raise IntegrityError(f"{r.url}: got {len(body)} of {size} bytes")
    return body


def reject_constant(name):
    raise ValueError(f"{name} is not valid JSON")


def finite_float(text):
    value = float(text)
    if not math.isfinite(value):
        raise ValueError(f"{text[:40]} is out of range")
    return value


class BadJSON(IntegrityError):
    """A body that is not valid JSON. Retried like any integrity error (a body cut short
    without a length looks the same); if it persists, the object itself is malformed."""


def read_json_body(r):
    body = read_body(r)
    try:
        return json.loads(body, parse_constant=reject_constant, parse_float=finite_float)
    except RecursionError:
        raise ValueError(f"{r.url}: JSON nested too deeply") from None
    except ValueError as e:  # also a body cut short without a length: retry
        raise BadJSON(f"{r.url}: bad JSON: {e}") from None


def retry_after(headers):
    """Seconds a 429/503 asks us to wait (delta-seconds or an HTTP date), None if absent or unusable."""
    value = (headers.get("Retry-After") or "").strip() if headers else ""
    if value.isascii() and value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if when.tzinfo is None:
        return None
    return max(0.0, when.timestamp() - time.time())


def request(url, consume=read_body, headers=None, timeout=120, method="GET"):
    """Fetch url with retries; consume(response) reads the body.

    Waits between attempts grow exponentially with jitter, honour Retry-After
    (up to RETRY_AFTER_CAP), end early when the run is abandoned, and are never
    allowed to outlast the run: a wait that would is skipped and the error
    raised, so the next run retries instead.
    """
    ctx = context()
    limit = ctx.limit_for(url)
    for attempt in range(RETRIES):
        if ctx.abandoned.is_set():
            raise Abandoned(url)
        limit.acquire()
        pushback, asked = False, None
        try:
            if ctx.abandoned.is_set():  # (it may have waited for its slot)
                raise Abandoned(url)
            return fetch_once(url, consume, headers, timeout, method)
        except urllib.error.HTTPError as e:
            e.close()
            if e.code == 404:
                raise NotFound(url) from None
            pushback = e.code in (429, 503, 504)
            if (e.code < 500 and e.code not in (408, 429)) or attempt == RETRIES - 1:
                raise
            if e.code in (429, 503):
                asked = retry_after(e.headers)
            failure = e
        except IntegrityError as e:
            if attempt == RETRIES - 1:
                raise
            failure = e
        except TRANSIENT as e:
            pushback = True
            if attempt == RETRIES - 1:
                raise
            failure = e
        finally:
            limit.release(pushback)
        wait = BACKOFF * 2 ** attempt * random.uniform(0.5, 1.5)
        if asked is not None:
            wait = max(wait, min(asked, RETRY_AFTER_CAP))
        if time.monotonic() + wait > ctx.deadline:
            raise failure  # waiting would outlast the run; the next run retries
        if ctx.abandoned.wait(wait):
            raise Abandoned(url)


def get_json(url):
    return request(url, read_json_body)


def quote(s):
    return urllib.parse.quote(s, safe="")


def table_url(testgrid, dashboard, tab):
    # Same request the dashboard page makes; the server applies the tab's base options.
    return f"{testgrid}/{quote(dashboard)}/table?tab={quote(tab)}&dashboard={quote(dashboard)}"


def created(build):
    return ((int(build) >> 22) + PROW_EPOCH_MS) // 1000


def iso(ts):
    return dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def plausible_time(t, now):
    """A Unix time in seconds that the archive can date and chart."""
    try:
        return (isinstance(t, (int, float)) and not isinstance(t, bool) and math.isfinite(t)
                and 0 < t <= now + FUTURE_SLACK)
    except OverflowError:  # an int too large for a float
        return False


def is_int(v):
    return type(v) is int  # not bool, not float


CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


def is_text(v, limit=4096):
    """A string safe to put in file names' neighbours, JSON, Markdown and SVG: valid
    UTF-8, no control characters (tab and newline allowed), not absurdly long."""
    if not isinstance(v, str) or len(v) > limit:
        return False
    try:
        v.encode("utf-8")
    except UnicodeEncodeError:  # a lone surrogate from a JSON escape
        return False
    return CONTROL_RE.search(v) is None


def texts_ok(items, limit):
    """Whether every item passes is_text(item, limit), checked in bulk: a table
    holds a message for every cell with a result, millions in a large one."""
    try:
        joined = "".join(items)  # a TypeError for any item that is not a string
    except TypeError:
        return False
    if len(joined) > limit and max(map(len, items)) > limit:  # only measured one by one if it can matter
        return False
    try:
        joined.encode("utf-8")
    except UnicodeEncodeError:  # a lone surrogate from a JSON escape
        return False
    return CONTROL_RE.search(joined) is None


_SURROGATE = re.compile("[" + chr(0xD800) + "-" + chr(0xDFFF) + "]")


def clean_text(v, limit):
    """A string from TestGrid as is_text wants it: control characters (such as a test
    output's carriage returns) become spaces, lone surrogates U+FFFD, cut at limit."""
    return CONTROL_RE.sub(" ", _SURROGATE.sub(chr(0xFFFD), v[:limit]))


# RLE lets a few bytes claim millions of cells: past this many red cells in one
# table only the first are listed (real tabs hold a few thousand), and past this
# many columns a table is rejected (real tabs show at most a few thousand).
MAX_RED_CELLS = 500_000
MAX_COLUMNS = 50_000


class Cells(list):
    """A column's red cells, of which `omitted` more are not listed (see red_columns)."""
    omitted = 0


def run_problem(name, runs, width):
    """Raise ValueError for what is wrong with a row's runs, checked one by one."""
    total = 0
    for r in runs:
        if not isinstance(r, dict):
            raise ValueError(f"row {name!r}: malformed run {str(r)[:80]!r}")
        value, count = r.get("value"), r.get("count")
        if not is_int(count) or count <= 0:
            raise ValueError(f"row {name!r}: run length {count!r} is not a positive integer")
        if not is_int(value) or value not in STATUSES:
            raise ValueError(f"row {name!r}: unknown status {value!r}")
        total += count
    raise ValueError(f"row {name!r} spans {total} columns, not {width}")


_RUN_VALUE, _RUN_COUNT = operator.itemgetter("value"), operator.itemgetter("count")
_STATUS_KEYS = frozenset(STATUSES)


def table_rows(table):
    """Validate a table's shape and run-length encoding; return (width, rows).

    Each row is (name, [(value, count), ...], messages). Anything TestGrid would
    never send is rejected rather than guessed at: a wrong type anywhere, a
    count that is not a positive int, a status outside TestGrid's TestStatus
    values, a row that does not span the table exactly, or a build listed twice.
    A row's runs are checked in bulk; a bad one is then found run by run.
    """
    if not isinstance(table, dict):
        raise ValueError(f"table is a {type(table).__name__}, not an object")
    changelists, stamps = table.get("changelists"), table.get("timestamps")
    if not isinstance(changelists, list) or not isinstance(stamps, list):
        raise ValueError("changelists and timestamps must be lists")
    width = len(changelists)
    if len(stamps) != width:
        raise ValueError(f"{width} changelists but {len(stamps)} timestamps")
    if width > MAX_COLUMNS:
        raise ValueError(f"{width} columns, more than the {MAX_COLUMNS} accepted")
    if len(set(map(str, changelists))) != width:
        raise ValueError("a build is listed in two columns")
    tests = table.get("tests", [])
    if not isinstance(tests, list):
        raise ValueError("tests must be a list")
    rows = []
    for test in tests:
        if not isinstance(test, dict) or not isinstance(test.get("name"), str) or len(test["name"]) > 1000:
            raise ValueError(f"malformed row {str(test)[:80]!r}")
        name, runs = test["name"], test.get("statuses")
        if not is_text(name, 1000):
            name = clean_text(name, 1000)
        if not isinstance(runs, list):
            raise ValueError(f"row {name!r}: statuses must be a list")
        try:
            values, counts = list(map(_RUN_VALUE, runs)), list(map(_RUN_COUNT, runs))
            ok = (set(map(type, values)) <= {int} and set(map(type, counts)) <= {int} and set(values) <= _STATUS_KEYS
                  and (not counts or min(counts) > 0) and sum(counts) == width)
        except (KeyError, TypeError):  # a run that is not an object, or lacks a field
            ok = False
        if not ok:
            run_problem(name, runs, width)
        messages = test.get("messages")
        if messages is None:
            messages = []
        if not isinstance(messages, list):
            raise ValueError(f"row {name!r}: messages must be a list of strings")
        if not texts_ok(messages, 1 << 20):
            if not all(isinstance(m, str) for m in messages):
                raise ValueError(f"row {name!r}: messages must be a list of strings")
            messages = [clean_text(m, 1 << 20) for m in messages]  # (a control character from a test's output)
        rows.append((name, list(zip(values, counts, strict=True)), messages))
    return width, rows


_INFORMATIVE = bytes(0 if v in (NO_RESULT, 6) else 1 for v in range(256))  # a status byte: is it a result?
_RUNNING = bytes(1 if v == 4 else 0 for v in range(256))  # ... is it RUNNING?
_STATUS_BYTE = [bytes((v,)) for v in range(16)]


def informative_columns(rows, width, which=_INFORMATIVE):
    """The columns where some row has a status `which` marks: by default a result (not
    NO_RESULT, not UNKNOWN: TestGrid's grey placeholder). A row with many runs is
    expanded in bulk (work per column, in C), one with few adds to a difference array
    (work per run)."""
    seen, diff = 0, [0] * (width + 1)
    for _, runs, _ in rows:
        if len(runs) < 4 + width // 100:  # (measured: below this, run by run is cheaper)
            col = 0
            for value, count in runs:
                if which[value]:
                    diff[col] += 1
                    diff[col + count] -= 1
                col += count
        else:
            expanded = b"".join(map(operator.mul, map(_STATUS_BYTE.__getitem__, map(_FIRST, runs)), map(_SECOND, runs)))
            seen |= int.from_bytes(expanded.translate(which), "little")
    flags, out, depth = seen.to_bytes(width, "little"), [], 0
    for i in range(width):
        depth += diff[i]
        if depth or flags[i]:
            out.append(i)
    return out


_FIRST, _SECOND = operator.itemgetter(0), operator.itemgetter(1)


def red_columns(table, rows=None):
    """Map column index -> red cells in that column (one column is one build).

    Rows are run-length encoded. `messages` only has entries for cells with a
    result, so a cell's message index skips the NO_RESULT cells before it.
    Counting is done in bulk, and only rows with a red run are walked. A table
    with more than MAX_RED_CELLS red cells (a mass failure) still gives every red
    column, with only its first red cells listed: the rest are counted in
    Cells.omitted (work and memory stay bounded however many cells RLE claims).
    """
    if rows is None:
        _, rows = table_rows(table)
    red = []
    total = 0
    for row in rows:
        runs = row[1]
        if RED.keys() & set(map(_FIRST, runs)):
            red.append(row)
            total += sum(itertools.compress(map(_SECOND, runs), map(RED.__contains__, map(_FIRST, runs))))
    budget = MAX_RED_CELLS
    reds, cover = {}, None if total <= budget else [0] * (len(table["changelists"]) + 1)
    for name, runs, messages in red:
        values, counts = list(map(_FIRST, runs)), list(map(_SECOND, runs))
        ends = list(itertools.accumulate(counts))  # each run's end column ...
        # ... and its end among the cells with a result, which are the ones with a message
        result_ends = list(itertools.accumulate(map(operator.mul, counts, map(NO_RESULT.__ne__, values))))
        aligned = len(messages) == (result_ends[-1] if result_ends else 0)
        for k in itertools.compress(range(len(values)), map(RED.__contains__, values)):
            count, status = counts[k], RED[values[k]]
            col, idx = ends[k] - count, result_ends[k] - count
            if cover is not None:  # red cells per column, counted as a difference array
                cover[col] += 1
                cover[col + count] -= 1
            for j in range(min(count, budget)):
                reds.setdefault(col + j, Cells()).append({"test": name, "status": status,
                                                          "message": messages[idx + j] if aligned else None})
            budget -= min(count, budget)
    if cover is not None:
        depth = 0
        for col in range(len(cover) - 1):
            depth += cover[col]
            if depth:
                cells = reds.setdefault(col, Cells())
                cells.omitted = depth - len(cells)
    return reds


def read_tab(table, now, running=None):
    """Validate one tab's table; return (query, job, oldest start, [(build, start, cells)],
    the builds whose columns hold results). running: a list to add (build, start) to for
    each column TestGrid shows as still RUNNING."""
    width, rows = table_rows(table)
    reds = red_columns(table, rows)
    query = table.get("query")
    if not isinstance(query, str) or not QUERY_RE.fullmatch(query):
        raise ValueError(f"unsupported GCS query {str(query)[:120]!r}")
    job = query.rsplit("/", 1)[-1]
    if not JOB_RE.fullmatch(job):
        raise ValueError(f"unexpected job name {job!r}")
    starts = []
    for stamp in table["timestamps"]:
        if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
            raise ValueError(f"implausible timestamp {stamp!r}")
        # 0 is TestGrid's marker for a column without a start time; any other time it cannot be is bogus
        starts.append(int(stamp) // 1000 if 0 < stamp <= (now + FUTURE_SLACK) * 1000 else None)
    for i, (build, start) in enumerate(zip(table["changelists"], starts, strict=True)):
        if not isinstance(build, str) or not BUILD_RE.fullmatch(build) or not plausible_time(created(build), now):
            raise ValueError(f"unexpected build id {str(build)[:40]!r}")
        if start is not None and start < created(build) - 86400:
            # TestGrid dates a column it could not read (TOOL_FAIL, red) by an earlier
            # column; such a start cannot be this build's, so it counts as unknown.
            starts[i] = None
    found = [(table["changelists"][col], starts[col], cells) for col, cells in sorted(reds.items())]
    # How far back the tab shows results: columns with nothing but NO_RESULT/UNKNOWN
    # (such as TestGrid's "grid exceeds maximum size" placeholder) do not count.
    informative = informative_columns(rows, width)
    known = [starts[i] for i in informative if starts[i] is not None]
    shown = {table["changelists"][i] for i in informative}
    if running is not None:
        running += [(table["changelists"][i], starts[i]) for i in informative_columns(rows, width, _RUNNING)]
    return query, job, (min(known) if known else None), found, shown


GO_DURATION = re.compile(r"([0-9]+(?:\.[0-9]+)?)(ms|us|µs|ns|h|m|s)")
GO_UNITS = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}


def go_duration(text):
    """Seconds in a Go duration such as "2h30m0s"; None if it is not one."""
    if not isinstance(text, str) or not text:
        return None
    pos, total = 0, 0.0
    for m in GO_DURATION.finditer(text):
        if m.start() != pos:
            return None
        total += float(m.group(1)) * GO_UNITS[m.group(2)]
        pos = m.end()
    return total if pos == len(text) else None


def prow_job_hours(gcs, query, build):
    """How long a build of this job can run, from Prow's decoration timeout plus
    its grace period and a quarter hour to upload; None if prowjob.json says nothing."""
    try:
        job = get_json(f"{gcs}/{build_folder(gcs, query, build)}/prowjob.json")
        decoration = job["spec"]["decoration_config"]
        timeout, grace = go_duration(decoration.get("timeout")), go_duration(decoration.get("grace_period") or "0s")
    except Exception:  # missing or unexpected: fall back to --max-job-hours
        return None
    if timeout is None or grace is None or not 0 < timeout + grace < 7 * 86400:
        return None
    return round((timeout + grace) / 3600 + 0.25, 2)


INDEX_RE = re.compile(r"(?:.*/)?pr-logs/directory/[^/]+")  # (the prefix of a query: see indexed)
LINK_PART_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]*")  # a part of a folder a link of Prow's index names
# what Go's strings.TrimSpace trims: Unicode's White_Space (str.strip() trims U+001C to U+001F too)
GO_SPACE = "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a" \
           "\u2028\u2029\u202f\u205f\u3000"
GO_HOST_CHARS = frozenset("!$&'()*+,;=:[]<>\"-_.~")  # (and letters and digits: net/url's shouldEscape, of a host)
SURROGATE_RE = re.compile(f"[{chr(0xD800)}-{chr(0xDFFF)}]")  # (what json.loads leaves of a lone surrogate escape)


def indexed(query):
    """Whether a TestGrid group's GCS path ("bucket/prefix") is Prow's index of a pull request job's
    builds, pr-logs/directory/<job>: an entry there per build, <build>.txt, links the build's folder
    (pr-logs/pull/...), where its files are."""
    return bool(INDEX_RE.fullmatch(query.partition("/")[2]))


def go_unescape(s, host=False):
    """s with its %XX escapes undone, as Go's net/url unescapes a URL's path (or host); None where
    url.Parse fails: a % not followed by two hex digits, and in a host an escaped ASCII byte (but %25)
    or an ASCII byte other than a letter, a digit or one of GO_HOST_CHARS (an @ too: url.Parse takes
    what comes before it for a user, which Path.SetURL refuses)."""
    raw, out, i = s.encode("utf-8", "surrogatepass"), bytearray(), 0
    while i < len(raw):
        if raw[i] == 0x25:  # "%"
            pair = raw[i + 1:i + 3].decode("ascii", "replace")
            if len(pair) < 2 or not all(c in "0123456789abcdefABCDEF" for c in pair) \
                    or host and int(pair[0], 16) < 8 and pair != "25":
                return None
            out.append(int(pair, 16))
            i += 3
            continue
        if host and raw[i] < 0x80 and not chr(raw[i]).isalnum() and chr(raw[i]) not in GO_HOST_CHARS:
            return None
        out.append(raw[i])
        i += 1
    return out.decode("utf-8", "surrogateescape")


def testgrid_link(link):
    """How TestGrid's ListBuilds (util/gcs/read.go) takes a link of Prow's index (see readLink): None
    for "" (no link); "fails" for one its listing fails on, as Go's url.Parse or TestGrid's
    Path.SetURL (util/gcs/gcs.go) refuses it; else (scheme, bucket, path) of the folder it lists a
    build at: path as url.Parse decodes it, ending in "/"."""
    if link == "":
        return None
    link = link.strip(GO_SPACE)  # (strings.TrimSpace: "links created by bootstrap.py have a space")
    rest, _, fragment = link.partition("#")
    if fragment or any(ord(c) < 0x20 or ord(c) == 0x7F for c in link):
        return "fails"  # (Path.SetURL takes no #fragment; url.Parse no control character)
    m = re.match(r"([A-Za-z][A-Za-z0-9+.-]*):", rest)
    scheme, rest = (m[1].lower(), rest[m.end():]) if m else ("", rest)
    rest, _, query = rest.partition("?")
    if query or scheme not in ("gs", "file", ""):
        return "fails"  # (Path.SetURL: no ?query, and gs://, file:// or a local path only)
    if not rest.startswith("/"):
        if scheme:  # (an opaque URL: Path.SetURL refuses it, unless it is empty)
            return "fails" if rest else (scheme, "", "/")
        if ":" in rest.partition("/")[0]:  # (also one that starts with ":", a missing protocol scheme)
            return "fails"  # (url.Parse: first path segment in URL cannot contain colon)
    bucket = ""
    if (scheme or not rest.startswith("///")) and rest.startswith("//"):
        authority, slash, path = rest[2:].partition("/")
        rest = slash + path
        bucket = go_unescape(authority, host=True) if ":" not in authority and "[" not in authority else None
        if bucket is None:  # (Path.SetURL takes no :port or [IPv6], nor user@, which go_unescape refuses)
            return "fails"
    path = go_unescape(rest)
    if path is None:
        return "fails"
    return scheme, bucket, path if path.endswith("/") else path + "/"


def link_folder(metadata):
    """The build an entry of Prow's index links, as TestGrid's ListBuilds reads its x-goog-meta-link
    metadata, else its link (see testgrid_link): None for an entry that links nothing; else (build,
    folder): the build TestGrid takes (the folder's last part), and its folder ("bucket/path"), or None
    for one this archive does not read: one TestGrid reads outside GCS (not gs://, or no bucket), and
    one with a %XX, or a part that is empty, "." or "..", or not a letter, a digit or "_" followed by
    letters, digits, ".", "_" and "-" (TestGrid decodes a %XX and resolves "." and ".." away; this
    archive reads a folder by its link as written, and puts no other character in a URL). Raises
    IntegrityError for a link TestGrid's listing fails on."""
    if not isinstance(metadata, dict):
        return None
    link = metadata["x-goog-meta-link"] if "x-goog-meta-link" in metadata else metadata.get("link")
    if isinstance(link, str):  # (a lone surrogate escape in GCS's JSON: Go's encoding/json reads U+FFFD)
        link = SURROGATE_RE.sub(chr(0xFFFD), link)
    taken = testgrid_link(link) if isinstance(link, str) else None if link is None else "fails"
    if taken == "fails":
        raise IntegrityError(f"a link in Prow's index that TestGrid cannot list: {str(link)[:200]!r}")
    if taken is None:
        return None
    scheme, bucket, path = taken
    obj = path.encode("utf-8", "surrogateescape")[1:-1]  # (TestGrid's Build.object(), of Go's bytes)
    base = obj.rstrip(b"/").rsplit(b"/", 1)[-1] if obj.rstrip(b"/") else b"/" if obj else b"."  # (path.Base)
    build, obj = base.decode("utf-8", "surrogateescape"), obj.decode("utf-8", "surrogateescape")
    # (no %XX: TestGrid decodes it, and for a link that ends in "/" resolves started.json against it as it came)
    readable = scheme == "gs" and "%" not in link and LINK_PART_RE.fullmatch(bucket) and path.startswith("/") \
        and all(LINK_PART_RE.fullmatch(part) for part in obj.split("/"))
    return build, f"{bucket}/{obj}" if readable else None


def remember_folder(gcs, query, build, folder, error=None):
    """Keep, for the rest of the run, where a build of Prow's index keeps its files (see build_folder),
    or why it cannot be read (error: the IntegrityError build_folder raises)."""
    ctx = context()
    with ctx._lock:
        ctx.folders[(gcs, query, build)] = (folder, error)


class NoSuchBuild(IntegrityError):
    """GCS holds neither an entry <build>.txt of Prow's index that links a folder, nor a folder <build>/
    under the index (see build_folder): for a build archived from there, GCS no longer has it, unless
    another entry links it."""


def build_folder(gcs, query, build):
    """Where a build of a TestGrid group (query: its GCS path) keeps its files, as "bucket/path":
    query/build, or, in Prow's index of a pull request job (see indexed), the folder its entry
    <build>.txt links (see link_folder), where TestGrid reads its files. Without an entry, or with one
    that links nothing, query/build, where TestGrid lists a folder as a build too, if GCS holds one.
    Raises IntegrityError for an entry that links another build (TestGrid takes the build from the
    link: that column is not this build's), or a folder this archive does not read, or a link TestGrid
    cannot list, and NoSuchBuild for a build with neither (TestGrid takes it from another entry's link,
    which only a listing of the index finds, or GCS no longer has it)."""
    if not indexed(query):
        return f"{query}/{build}"
    ctx = context()
    with ctx._lock:
        known = ctx.folders.get((gcs, query, build))
    if known is None:
        bucket, _, prefix = query.partition("/")
        try:
            entry = get_json(f"{gcs}/storage/v1/b/{quote(bucket)}/o/{quote(f'{prefix}/{build}.txt')}?fields=metadata")
        except NotFound:
            entry = None
        try:
            linked = link_folder(entry.get("metadata") if isinstance(entry, dict) else None)
            if linked and linked[0] != build:
                raise IntegrityError(f"gs://{query}/{build}.txt links build {linked[0][:40]!r}, another build")
            if linked and linked[1] is None:
                raise IntegrityError(f"gs://{query}/{build}.txt links a folder this archive does not read")
            if not linked and not holds_objects(gcs, bucket, f"{prefix}/{build}/"):
                raise NoSuchBuild(f"gs://{query}/ has neither an entry {build}.txt that links a folder, nor "
                                  f"a folder {build}/ (another entry may link it: a listing of the index says)")
            known = (linked[1] if linked else f"{query}/{build}", None)
        except IntegrityError as e:
            known = (None, e)
        remember_folder(gcs, query, build, *known)
    if known[1] is not None:
        raise type(known[1])(*known[1].args)  # (a new one each time: other threads may raise it too)
    return known[0]


def holds_objects(gcs, bucket, folder):
    """Whether GCS holds an object under gs://bucket/folder (one listed is enough)."""
    params = {"prefix": folder, "maxResults": "1", "fields": "items(name)"}
    try:
        page = get_json(f"{gcs}/storage/v1/b/{quote(bucket)}/o?{urllib.parse.urlencode(params)}")
    except NotFound:
        return False
    items = page.get("items", []) if isinstance(page, dict) else None
    if not isinstance(items, list):
        raise IntegrityError(f"unexpected GCS listing for gs://{bucket}/{folder}")
    return bool(items)


def stored_folder(meta):
    """The folder a build's meta.json records it was read from in Prow's index (gcs_path), if it is
    one build_folder could give for that build; else None."""
    path = meta.get("gcs_path") if isinstance(meta, dict) else None
    parts = path.split("/") if isinstance(path, str) else []
    return path if len(parts) >= 2 and all(LINK_PART_RE.fullmatch(part) for part in parts) \
        and parts[-1] == meta.get("build") else None


def listed_builds(page, prefix, query):
    """{build ID: its folder ("bucket/path"), or None} in a page of a GCS listing of gs://<query>/ by
    "/": its folders, and, in Prow's index of a pull request job (see indexed), the builds its entries
    link (see link_folder; None: a folder this archive does not read). Raises IntegrityError for a
    page that is not a listing, or an entry with a link TestGrid's listing fails on."""
    prefixes = page.get("prefixes", []) if isinstance(page, dict) else None
    items = page.get("items", []) if isinstance(page, dict) else None
    if not isinstance(prefixes, list) or not isinstance(items, list):
        raise IntegrityError(f"unexpected GCS listing for gs://{query}/")
    bucket, out = query.partition("/")[0], {}
    for p in prefixes:
        build = p[len(prefix):].rstrip("/") if isinstance(p, str) and p.startswith(prefix) else ""
        if build:
            out[build] = f"{bucket}/{prefix}{build}"
    for item in items if indexed(query) else ():
        linked = link_folder(item.get("metadata") if isinstance(item, dict) else None)
        if linked is not None:
            out[linked[0]] = linked[1]
    return out


def remember_listed(gcs, query, build, folder):
    """Keep where a listing said a build of Prow's index keeps its files (see listed_builds)."""
    if indexed(query):
        remember_folder(gcs, query, build, folder, None if folder else
                        IntegrityError(f"Prow's index links build {build} at a folder this archive does not read"))


def list_build_ids(gcs, query, lo, hi):
    """Build IDs under gs://<query>/ whose IDs say they were created in [lo, hi] (seconds): its folders,
    and in Prow's index of a pull request job, its entries' (see listed_builds)."""
    if lo > hi:
        return []  # nothing to search: TestGrid still shows everything since the earliest searchable time
    bucket, _, prefix = query.partition("/")
    prefix += "/"
    first = (max(0, int(lo * 1000) - PROW_EPOCH_MS)) << 22
    last = (max(0, int(hi * 1000) + 1001 - PROW_EPOCH_MS)) << 22  # (IDs of the next second on: past hi)
    ids, seen, token = [], set(), None
    for _ in range(MAX_LIST_PAGES):
        # (names are compared as strings, which orders IDs only among those of as many digits: with
        # first and last of as many, every ID in between is in the range, and the check below keeps
        # [lo, hi] exactly; else, before 2018-05-25 or around 2086, every build is listed. An entry of
        # Prow's index, <build>.txt, sorts as its build does.)
        params = {"prefix": prefix, "delimiter": "/",
                  "fields": "prefixes,items(name,metadata),nextPageToken" if indexed(query) else "prefixes,nextPageToken"}
        if len(str(first)) == len(str(last)):
            params.update(startOffset=f"{prefix}{first}", endOffset=f"{prefix}{last}")
        if token:
            params["pageToken"] = token
        page = get_json(f"{gcs}/storage/v1/b/{quote(bucket)}/o?{urllib.parse.urlencode(params)}")
        for build, folder in listed_builds(page, prefix, query).items():
            if BUILD_RE.fullmatch(build) and lo <= created(build) <= hi and build not in seen:
                ids.append(build)
                seen.add(build)
                remember_listed(gcs, query, build, folder)
        token = page.get("nextPageToken")
        if not token:
            return ids
    raise IntegrityError(f"the GCS listing of gs://{query}/ did not end after {MAX_LIST_PAGES} pages")


def scan(testgrid, gcs, dashboards, pool, deadline, check=None):
    """Read every tab of every dashboard.

    Returns (tab reports, red builds by (job, build), every build each query
    shows, builds shown as still running by (job, build), every build each tab
    read shows, by "dashboard#tab"). A tab's report says
    how far back it shows results ("oldest") and how long its job can run
    ("max_job_hours", from prowjob.json when known). check(dashboard, tab, table,
    query): how the tab's config disagrees with its table ("" when it agrees, None
    when it has no config), the report's "config_disagrees".
    """
    now = time.time()
    tabs, reports = [], []
    summaries = {d: pool.submit(get_json, f"{testgrid}/{quote(d)}/summary") for d in dashboards}
    cf.wait(summaries.values(), timeout=max(0, deadline - time.monotonic()))
    for dashboard, fut in summaries.items():
        try:
            if not fut.done():
                context().abandon()
                raise TimeoutError("run timeout reached before the summary loaded")
            summary = fut.result()
            if not isinstance(summary, dict) or not summary:
                raise ValueError(f"summary lists no tabs: {str(summary)[:80]!r}")
        except Exception as e:
            reports.append({"dashboard": dashboard, "tab": None, "error": f"summary: {type(e).__name__}: {e}"})
            continue
        for tab, info in sorted(summary.items()):
            if not is_text(tab, 300) or not tab.strip() or "\n" in tab or "\t" in tab:
                reports.append({"dashboard": dashboard, "tab": None, "error": f"summary: unusable tab name {tab[:80]!r}"})
                continue
            status = info.get("overall_status") if isinstance(info, dict) else None
            tabs.append((dashboard, tab, status if is_text(status, 40) else None))

    futures = [pool.submit(get_json, table_url(testgrid, d, t)) for d, t, _ in tabs]
    cf.wait(futures, timeout=max(0, deadline - time.monotonic()))
    if not all(f.done() for f in futures):
        context().abandon()
    builds, visible, newest, running, shown_on = {}, {}, {}, {}, {}
    for (dashboard, tab, status), fut in zip(tabs, futures, strict=True):
        report_ = {"dashboard": dashboard, "tab": tab, "status": status}
        reports.append(report_)
        still = []
        try:
            if not fut.done():
                raise TimeoutError("run timeout reached before the table loaded")
            query, job, oldest, found, shown = read_tab(fut.result(), now, still)
            for build, _, _ in found:
                other = builds.get((job, build))
                if other and other["query"] != query:
                    raise ValueError(f"{job}/{build} is also listed under {other['query']}")
        except Exception as e:
            report_["error"] = f"{type(e).__name__}: {e}"
            continue
        columns = fut.result()["changelists"]
        report_.update(query=query, columns=len(columns), oldest=oldest, red_builds=len(found),
                       rows=tab_rows(fut.result(), job, results=bool(shown)))
        disagrees = check(dashboard, tab, fut.result(), query) if check else None
        if disagrees is not None:  # ("" when the table agrees with the tab's config)
            report_["config_disagrees"] = disagrees
        visible.setdefault(query, set()).update(shown)
        shown_on[f"{dashboard}#{tab}"] = set(shown)
        if columns:
            newest[id(report_)] = pool.submit(prow_job_hours, gcs, query, columns[0])
        for build, started, cells in found:
            rec = builds.setdefault((job, build), {"job": job, "build": build, "query": query,
                                                   "started": started, "created": created(build), "tabs": []})
            rec["tabs"].append({"dashboard": dashboard, "tab": tab, "tab_status": status, "red_cells": cells,
                                **({"red_cells_omitted": cells.omitted} if getattr(cells, "omitted", 0) else {})})
        for build, started in still:
            rec = running.setdefault((job, build), {"job": job, "build": build, "query": query, "started": started,
                                                    "created": created(build), "tabs": [], "watch": True})
            rec["tabs"].append({"dashboard": dashboard, "tab": tab, "tab_status": status, "red_cells": []})
    cf.wait(newest.values(), timeout=max(0, min(deadline - time.monotonic(), PROWJOB_WAIT)))
    for r in reports:
        fut = newest.get(id(r))
        if fut and fut.done() and fut.exception() is None and fut.result():
            r["max_job_hours"] = fut.result()
    return reports, builds, visible, running, shown_on


def gcs_hashes(headers):
    """The x-goog-hash checksums as hex, e.g. {"md5": ..., "crc32c": ...}; malformed ones are an IntegrityError."""
    out = {}
    for value in headers.get_all("x-goog-hash") or []:
        for part in value.split(","):
            k, _, v = part.strip().partition("=")
            if k in ("md5", "crc32c"):
                try:
                    digest = base64.b64decode(v, validate=True)
                except ValueError:
                    raise IntegrityError(f"bad x-goog-hash {k} value {v[:40]!r}") from None
                if len(digest) != (16 if k == "md5" else 4):
                    raise IntegrityError(f"bad x-goog-hash {k} length {len(digest)}")
                out[k] = digest.hex()
    return out


def _crc32c_table():
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
        table.append(c)
    return table


CRC32C_TABLE = _crc32c_table()


def crc32c(data, crc=0):
    """CRC32C (Castagnoli), as GCS reports it. Pure Python and slow, so it is only
    used for an object GCS gives no md5 for."""
    crc ^= 0xFFFFFFFF
    table = CRC32C_TABLE
    for b in data:
        crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def gcs_md5(headers):
    return gcs_hashes(headers).get("md5")


def remote_identity(headers):
    """What GCS says about a stored object: enough to tell later whether it changed."""
    hashes = gcs_hashes(headers)
    generation = headers.get("x-goog-generation")
    return {"generation": generation if generation and generation.isascii() and generation.isdigit() else None,
            "md5": hashes.get("md5"), "crc32c": hashes.get("crc32c"),
            "bytes": header_int(headers, "x-goog-stored-content-length"),
            "encoding": headers.get("x-goog-stored-content-encoding", "identity")}


def recorded_identity(info):
    """The GCS identity recorded for an archived file. Records written before
    identities were kept have only the raw md5 and length, which equal GCS's
    for an object stored without encoding."""
    if isinstance(info.get("gcs"), dict):
        return info["gcs"]
    return {"generation": None, "md5": info.get("md5"), "crc32c": None, "bytes": info.get("bytes"),
            "encoding": "identity"}


def same_object(a, b):
    """Whether two GCS identities describe the same content; unknown counts as changed."""
    if a.get("encoding") != b.get("encoding"):
        return False
    for key in ("md5", "crc32c"):
        if a.get(key) and b.get(key):
            return a[key] == b[key] and a.get("bytes") == b.get("bytes")
    return bool(a.get("generation")) and a.get("generation") == b.get("generation")


class Gunzip:
    """Streaming gunzip, strict like gzip(1): concatenated members and trailing
    zero padding are fine; an empty stream, padding before any member, garbage
    after one, or a member cut short are IntegrityErrors.

    feed() is a generator yielding at most GUNZIP_STEP bytes at a time, so a
    highly compressible stream never needs more memory than that; finish()
    checks the stream ended cleanly.
    """

    def __init__(self):
        self.members = 0  # complete members, trailer CRC and length checked by zlib
        self._padding = False
        self._new()

    def _new(self):
        self._d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._open = False  # bytes of the current member have been fed

    def feed(self, data):
        try:
            while data:
                if not self._open and not data.strip(b"\0"):
                    if not self.members:
                        raise IntegrityError("zero bytes where a gzip member should start")
                    self._padding = True  # zeros after the last member
                    return
                if self._padding:
                    raise IntegrityError("data after gzip padding")
                self._open = True
                out = self._d.decompress(data, GUNZIP_STEP)
                if out:
                    yield out
                if self._d.eof:
                    self.members += 1
                    data = self._d.unused_data
                    self._new()
                elif self._d.unconsumed_tail:
                    data = self._d.unconsumed_tail
                elif len(out) == GUNZIP_STEP:
                    data = b""
                    yield from self._drain()
                else:
                    data = b""
        except zlib.error as e:
            raise IntegrityError(f"bad gzip data: {e}") from None

    def _drain(self):
        """Output zlib holds back after taking all input (only when a step filled up)."""
        while True:
            out = self._d.decompress(b"", GUNZIP_STEP)
            if out:
                yield out
            if self._d.eof:
                self.members += 1
                rest = self._d.unused_data
                self._new()
                if rest:
                    yield from self.feed(rest)
                return
            if len(out) < GUNZIP_STEP:
                return

    def finish(self):
        """Check the stream ended cleanly (feed() has already yielded all output)."""
        if self._open and not self._d.eof:
            raise IntegrityError("truncated gzip stream")
        if not self.members:
            raise IntegrityError("empty gzip stream: no complete member")


def gunzipped(chunks_):
    """Decompress an iterable of gzip chunks, strictly and in bounded steps."""
    g = Gunzip()
    for chunk in chunks_:
        yield from g.feed(chunk)
    g.finish()


def refuse_if_abandoned(what):
    # After a run timeout only the main thread may still write to the archive.
    if context().abandoned.is_set() and threading.current_thread() is not threading.main_thread():
        raise Abandoned(what)


def temp_path(path):
    refuse_if_abandoned(path)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=f".{os.path.basename(path)}.", suffix=".part")
    os.fchmod(fd, 0o666 & ~UMASK)  # mkstemp makes 0600; give archive files the usual mode
    os.close(fd)
    return tmp


def replace(src, dst):
    with context().write_lock:
        refuse_if_abandoned(dst)
        os.replace(src, dst)


def remove(path):
    """Remove a file of the archive if it is there (like replace(), refused to a worker of an abandoned run)."""
    with context().write_lock:
        refuse_if_abandoned(path)
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)


class Staged:
    """A verified copy of a build's file (name: build-log.txt or podinfo.json) in temp files of its folder,
    for put_in_place: temps, [(temp file, the name it takes)], one for the file whole or one per part (see
    split_parts); info, what meta.json records of it."""

    def __init__(self, name, info, temps):
        self.name, self.info, self.temps = name, info, temps

    def discard(self):
        remove_temps(self.temps)


def download(url, path, compress=False, max_bytes=None):
    """Fetch url to temp files next to path, checking length and md5 against GCS: a Staged copy, which
    put_in_place puts in place as path (gzipped, as path.gz, if compress), or as numbered parts if it is
    larger than max_bytes (see split_parts).

    Its info's md5 and byte count describe the raw content either way.
    """
    final = path + ".gz" if compress else path
    try:
        part = temp_path(final)
    except OSError as e:  # (a disk error, as one writing it is)
        raise LocalIOError(f"{final}: {e}") from None
    made = []  # (the copy an attempt made: removed if the request is tried again after it)

    def consume(r):
        while made:
            made.pop().discard()
        gcs = remote_identity(r.headers)
        stored_enc = gcs["encoding"]
        sent_enc = r.headers.get("Content-Encoding", "identity")
        if sent_enc not in ("identity", "gzip"):
            raise IntegrityError(f"{url}: unsupported Content-Encoding {sent_enc!r}")
        sent_len = header_int(r.headers, "Content-Length")
        if sent_len is not None and sent_len > MAX_DOWNLOAD_BYTES:
            raise IntegrityError(f"{url}: {sent_len} bytes is more than the {MAX_DOWNLOAD_BYTES} byte limit")
        raw, raw_len, out, out_len = hashlib.md5(), 0, hashlib.md5(), 0
        gunzip = Gunzip() if sent_enc == "gzip" else None
        # Without an md5 (a composite object), check GCS's crc32c of the stored bytes instead.
        crc_of = None if gcs["md5"] or not gcs["crc32c"] else "raw" if sent_enc == stored_enc else "out"
        crc = {"raw": 0, "out": 0}
        try:
            f = open(part, "wb")
        except OSError as e:
            raise LocalIOError(f"{part}: {e}") from None
        # a fixed name and mtime keep the .gz bytes identical for the same log
        sink = gzip.GzipFile(filename=os.path.basename(final), fileobj=f, mode="wb", compresslevel=6, mtime=0) \
            if compress else f

        def write(data):
            nonlocal out_len
            try:
                sink.write(data)
            except OSError as e:
                raise LocalIOError(f"{part}: {e}") from None
            out.update(data)
            out_len += len(data)
            if crc_of == "out":
                crc["out"] = crc32c(data, crc["out"])
        try:
            for chunk in chunks(r, url):
                raw.update(chunk)
                raw_len += len(chunk)
                if raw_len > MAX_DOWNLOAD_BYTES:
                    raise IntegrityError(f"{url}: more than the {MAX_DOWNLOAD_BYTES} byte limit")
                if crc_of == "raw":
                    crc["raw"] = crc32c(chunk, crc["raw"])
                if gunzip:
                    for piece in gunzip.feed(chunk):
                        if out_len + len(piece) > MAX_DOWNLOAD_BYTES:  # (a few bytes can gunzip to many)
                            raise IntegrityError(f"{url}: more than the {MAX_DOWNLOAD_BYTES} byte limit once gunzipped")
                        write(piece)
                else:
                    write(chunk)
            if gunzip:
                gunzip.finish()
            try:  # closing flushes buffers and writes the gzip trailer: a disk error, not a network one
                sink.close()
                f.close()
            except OSError as e:
                raise LocalIOError(f"{part}: {e}") from None
        finally:
            for handle in (sink, f):
                with contextlib.suppress(OSError, ValueError):
                    handle.close()
        if sent_len is not None and sent_len != raw_len:
            raise IntegrityError(f"{url}: got {raw_len} of {sent_len} bytes")
        # GCS's length and md5 describe the bytes in their stored encoding.
        if sent_enc == stored_enc:
            got_md5, got_len = raw.hexdigest(), raw_len
        elif stored_enc == "identity":
            got_md5, got_len = out.hexdigest(), out_len
        else:
            raise IntegrityError(f"{url}: served as {sent_enc!r} but stored as {stored_enc!r}")
        if gcs["bytes"] is not None and gcs["bytes"] != got_len:
            raise IntegrityError(f"{url}: got {got_len} bytes, GCS stores {gcs['bytes']}")
        if gcs["md5"] is not None and gcs["md5"] != got_md5:
            raise IntegrityError(f"{url}: got md5 {got_md5}, GCS stores {gcs['md5']}")
        if crc_of and f"{crc[crc_of]:08x}" != gcs["crc32c"]:
            raise IntegrityError(f"{url}: got crc32c {crc[crc_of]:08x}, GCS stores {gcs['crc32c']}")
        try:
            file_bytes = os.path.getsize(part)
            temps, parts = split_parts(part, final, max_bytes)
        except OSError as e:
            raise LocalIOError(f"{final}: {e}") from None
        info = {"bytes": out_len, "md5": out.hexdigest(),
                "verified": gcs["bytes"] is not None and (gcs["md5"] is not None or crc_of is not None),
                "file": os.path.basename(final), "file_bytes": file_bytes, "gcs": gcs}
        made.append(Staged(os.path.basename(path), dict(info, parts=parts) if parts else info, temps))
        return made[0]

    try:
        # Accept gzip so a gzip-stored object arrives exactly as stored and stays verifiable.
        return request(url, consume, headers={"Accept-Encoding": "gzip"})
    except BaseException:
        while made:
            made.pop().discard()
        raise
    finally:
        if not made:  # (one made into a copy is the copy, or went to its parts)
            with contextlib.suppress(OSError):
                os.remove(part)


def json_text(data):
    return json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n"


def write_json(path, data):
    write_file(path, json_text(data))


def write_new_json(path, data):
    """write_json, but only where no file (nor link) is yet, else FileExistsError: the file is
    never there empty, nor half written."""
    tmp = temp_path(path)
    try:
        with open(tmp, "w", encoding="utf-8", errors="backslashreplace") as f:
            f.write(json_text(data))
        with context().write_lock:
            refuse_if_abandoned(path)
            os.link(tmp, path)  # (refused for any name there, a link to nothing too)
    finally:
        with contextlib.suppress(FileNotFoundError):  # (a fetch sweeping temp files may have taken it)
            os.remove(tmp)


def write_file(path, text):
    tmp = temp_path(path)
    try:
        with open(tmp, "w", encoding="utf-8", errors="backslashreplace") as f:  # a lone surrogate stays visible
            f.write(text)
        replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def read_json(path):
    """Parsed JSON, None if the file is missing; raises ValueError if it is corrupt."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f, parse_constant=reject_constant, parse_float=finite_float)
    except FileNotFoundError:
        return None
    except RecursionError:
        raise ValueError(f"{path}: JSON nested too deeply") from None


def remove_if_empty(path):
    try:
        with context().write_lock:
            refuse_if_abandoned(path)  # after a timeout or interrupt only the main thread changes the archive
            os.rmdir(path)
    except (OSError, Abandoned):
        pass


def listdir(path):
    """os.listdir, but a folder that is gone (or is not one) lists as empty."""
    try:
        return os.listdir(path)
    except (FileNotFoundError, NotADirectoryError):
        return []


def remove_build_dir(target):
    """Remove a build folder left empty by a failed download. Its job folder stays, as
    another worker may be making a folder in it; git ignores it, and the next run
    removes it (clean_partial_builds)."""
    remove_if_empty(target)


def file_digest(path, parts=None):
    """(md5, length) of a file's raw content, read from its parts when it has them; a
    .gz file is gunzipped strictly, in bounded steps."""
    def data():
        for p in parts or [path]:
            with open(p, "rb") as f:
                yield from iter(lambda f=f: f.read(1 << 20), b"")
    md5, size = hashlib.md5(), 0
    for chunk in gunzipped(data()) if path.endswith(".gz") else data():
        md5.update(chunk)
        size += len(chunk)
    return md5.hexdigest(), size


def part_paths(target, name, parts):
    """The on-disk paths of a stored file's parts, checked to be name.000, name.001, ... in order."""
    if not isinstance(parts, list) or not parts:
        raise ValueError(f"no parts listed for {name}")
    paths = []
    for i, part in enumerate(parts):
        if not isinstance(part, dict) or part.get("file") != f"{name}.{i:03d}" or not is_int(part.get("bytes")):
            raise ValueError(f"unexpected part {str(part)[:80]!r} of {name}")
        paths.append(os.path.join(target, part["file"]))
    return paths


def file_md5(path):
    return file_digest(path)[0]


def intact(target, meta, deep=False):
    """Whether the files meta.json describes are on disk with the recorded size
    (and, if deep, the recorded raw md5 and length)."""
    for default, key in (("build-log.txt", "log"), ("podinfo.json", "podinfo")):
        info = meta.get(key)
        if info:
            try:
                if info.get("omitted"):
                    continue  # (an older record: verified but not stored, as too large for git)
                name = info.get("file", default)
                if name not in LOG_FILES:
                    return False
                path = os.path.join(target, name)
                parts = part_paths(target, name, info["parts"]) if "parts" in info else None
                sizes = [os.path.getsize(x) for x in parts] if parts else [os.path.getsize(path)]
                if sum(sizes) != info.get("file_bytes", info["bytes"]) or \
                        parts and sizes != [x["bytes"] for x in info["parts"]]:
                    return False
                if deep and file_digest(path, parts) != (info["md5"], info["bytes"]):
                    return False
            except (OSError, IntegrityError, KeyError, TypeError, AttributeError, ValueError):
                return False
    return True


def valid_meta(meta):
    return (isinstance(meta, dict) and isinstance(meta.get("tabs"), list)
            and all(isinstance(t, dict) and "dashboard" in t and "tab" in t for t in meta["tabs"]))


def cells_left_out(tab):
    """How many red cells of the build a tab TestGrid showed counts but does not list (a mass failure's, see
    red_columns), else 0. Only an int above 0 is a count, and none above MAX_JSON_BYTES: no table could hold
    that many rows, and a build has one cell a row. A tab judged from GCS counts no cell it does not list."""
    n = tab.get("red_cells_omitted")
    return n if is_int(n) and 0 < n <= MAX_JSON_BYTES and not tab.get("judged_from_gcs") else 0


def merge_tabs(old, new):
    """Old tab sightings plus any tab this build is newly red on. The first sighting
    wins, except that one taken from GCS in a history gap (judged by TestGrid's
    rule, or from before that, without red cells), or one TestGrid showed with every
    red cell left out (see cells_left_out), takes the cells TestGrid shows later. One
    with every cell left out lists none to take, so it takes the place of none: the
    cells judged from GCS stay, and the index cannot tell TestGrid showed the build there."""
    def from_gcs(t):
        return t.get("judged_from_gcs") or not t.get("red_cells")
    cells = {(t["dashboard"], t["tab"]): t for t in new if not from_gcs(t)}
    merged = [cells.get((t["dashboard"], t["tab"]), t) if from_gcs(t) else t for t in old]
    seen = {(t["dashboard"], t["tab"]) for t in old}
    added = [t for t in new if (t["dashboard"], t["tab"]) not in seen]
    return merged + added, bool(added) or merged != old


PART_STEP = 1 << 20  # parts are copied this much at a time


def remove_temps(temps):
    """Remove the temp files of [(temp file, the name it takes)] that were not put in place (one that cannot be
    removed is the next run's to sweep: see sweep_parts)."""
    for tmp, _ in temps:
        with contextlib.suppress(OSError):
            os.remove(tmp)


def split_parts(tmp, final, max_bytes=None):
    """How a verified download, in temp file tmp, goes in place: ([(temp file, the name it takes)], its parts).
    Whole, as `final` ([(tmp, final)], no parts), or, if it is larger than max_bytes, as numbered parts
    final.000, final.001, ... of at most max_bytes each (GitHub rejects files over 100 MB), copied to temp
    files of their own (tmp then goes): meta.json lists them, and `cat <file>.* > <file>` puts the file back
    together. A file too large for git so never sits under a name git commits."""
    size = os.path.getsize(tmp)
    if max_bytes is None or size <= max_bytes:
        return [(tmp, final)], []
    if size > 1000 * max_bytes:  # (names run .000 to .999, so `cat <file>.*` keeps their order)
        raise LocalIOError(f"{final}: {size} bytes would take more than 1000 parts of {max_bytes}")
    parts, staged = [], []
    try:
        with open(tmp, "rb") as f:
            while data := f.read(min(PART_STEP, max_bytes)):
                part = f"{final}.{len(parts):03d}"
                staged.append((temp_path(part), part))
                written = 0
                with open(staged[-1][0], "wb") as out:
                    while data:
                        out.write(data)
                        written += len(data)
                        data = f.read(min(PART_STEP, max_bytes - written)) if written < max_bytes else b""
                parts.append({"file": os.path.basename(part), "bytes": written})
        os.remove(tmp)
    except BaseException:
        remove_temps(staged)
        raise
    return staged, parts


def take_copy(base, target, compress, max_bytes, name="build-log.txt"):
    """A Staged copy of the build's build-log.txt (or `name`, as podinfo.json) from GCS, at base, in temp
    files of its folder, target (see download)."""
    return download(f"{base}/{name}", os.path.join(target, name), compress, max_bytes)


def drop_other_copies(target, name, info):
    """Remove the stored copies of name in a build folder (as it came or gzipped, whole or in parts) other
    than info's. One that cannot be removed (or listed) stays, a stray the cleanup removes or reports (see
    clean_partial_builds)."""
    keep = {p["file"] for p in info["parts"]} if info.get("parts") else {info["file"]}
    try:
        names = listdir(target)
    except OSError:
        names = []
    for n in names:
        m = PART_RE.fullmatch(n)
        if (m.group(1) if m else n) in (name, name + ".gz") and n not in keep:
            with contextlib.suppress(OSError):
                remove(os.path.join(target, n))


def put_in_place(target, meta, copies=()):
    """Write a build's meta.json, and put in place with it the Staged copies of its files it lists: all under the
    write lock (abandon() waits for it), and by renames only, once every byte is written (meta.json's too: a
    full disk stops it before anything changes). The copies go first to the names not there yet (a full disk
    can refuse a new name), then over the files there, and meta.json last; then the copies of the same files
    that it no longer lists (in the other form, or split otherwise) go. A rename refused part-way so changes no
    file the old meta.json lists, unless one was renamed over before it (the new names taken are strays the
    cleanup removes). The temp files go either way."""
    path, tmp = os.path.join(target, "meta.json"), None
    try:
        tmp = temp_path(path)
        with open(tmp, "w", encoding="utf-8", errors="backslashreplace") as f:  # (as write_file writes it)
            f.write(json_text(meta))
        with context().write_lock:  # (the first rename is refused once the run is abandoned: then none is done)
            renames = sorted((t for c in copies for t in c.temps), key=lambda t: os.path.lexists(t[1]))
            for src, dst in renames + [(tmp, path)]:
                replace(src, dst)
            for c in copies:
                drop_other_copies(target, c.name, c.info)
    except OSError as e:
        raise LocalIOError(f"{target}: {e}") from None
    finally:  # (one that cannot be removed is the next run's to sweep: see sweep_parts)
        for c in copies:
            c.discard()
        if tmp:
            with contextlib.suppress(OSError):
                os.remove(tmp)


def stray_files(target, meta):
    """Stored-file names in a build folder that its meta.json does not list: what a copy
    cut off part-way leaves (parts of a newer copy, or a copy in the other form). None at all
    if the meta.json names a file otherwise than intact() reads names: it cannot say which are its own."""
    listed = []
    for default, key in (("build-log.txt", "log"), ("podinfo.json", "podinfo")):
        info = meta.get(key)
        if info and not isinstance(info, dict):  # (one intact() cannot read)
            return []
        if info and not info.get("omitted"):  # (as intact() reads it)
            name = info.get("file", default)
            if not isinstance(name, str) or name not in LOG_FILES:
                return []
            if "parts" in info:  # (name.000, name.001, ..., as part_paths() reads them)
                parts = info["parts"]
                names = [p.get("file") if isinstance(p, dict) else None for p in parts] if isinstance(parts, list) else []
                if not names or names != [f"{name}.{i:03d}" for i in range(len(names))]:
                    return []
                listed += names
            else:
                listed.append(name)
    return sorted(n for n in listdir(target) if (n in LOG_FILES or PART_RE.fullmatch(n)) and n not in set(listed))


class RecheckError(Exception):
    """Comparing an intact archived build with GCS failed; its copy stays (but where the disk failed as a file
    was renamed over one there, see put_in_place: the next run then repairs it) and is compared again by the
    next run (the run is not done with its sevenths: see gcs_sevenths). meta says what the run took again of
    it before the failure: such a file is in place (see put_in_place)."""

    def __init__(self, message, meta):
        super().__init__(message)
        self.meta = meta


class NotCompared(Exception):
    """Comparing with GCS a build the runs gave up on failed again, on GCS's side (see COMPARISON_FAILING): no
    error of the run, and it is compared again in its seventh. The message says why, and meta what the run took
    again of it before the failure (as RecheckError's does)."""

    def __init__(self, message, meta):
        super().__init__(message)
        self.meta = meta


def read_finished(base):
    """The result and finish time to record for a build, from its finished.json;
    None while GCS has none. A field that is not a usable result string or Unix
    time is None (and a finished.json that is not JSON gives neither). One that
    cannot be fetched raises."""
    try:
        finished = get_json(base + "/finished.json")
    except NotFound:
        return None
    except (BadJSON, ValueError):  # it exists, so the build is over, but it is not JSON (a failed read is raised)
        finished = None
    finished = finished if isinstance(finished, dict) else {}
    result, stamp = finished.get("result"), finished.get("timestamp")
    return {"result": result if is_text(result, 40) else None,
            "timestamp": stamp if isinstance(stamp, (int, float)) and not isinstance(stamp, bool)
            and plausible_time(stamp, time.time()) else None}


MAX_JUNIT_BYTES = 256 << 20  # a junit file larger than this (only one stored gzipped can be) is not judged here


def read_artifact(url, limit, when=False):
    """An object's bytes as TestGrid's GCS client reads them (gunzipped when GCS
    serves them gzipped), None when there is no such object. With when: (its bytes,
    when GCS says it was last written (Last-Modified), or None)."""
    def consume(r):
        encoding = (r.headers.get("Content-Encoding") or "identity").strip().lower()
        if encoding == "identity":
            body = read_body(r, limit)
        elif encoding != "gzip":
            raise IntegrityError(f"{r.url}: unsupported Content-Encoding {encoding!r}")
        else:
            out, total = [], 0
            for piece in gunzipped(chunks(r, r.url)):
                total += len(piece)
                if total > limit:
                    raise IntegrityError(f"{r.url}: more than the {limit} byte limit")
                out.append(piece)
            body = b"".join(out)
        return (body, last_modified(r.headers)) if when else body
    try:
        return request(url, consume=consume)
    except NotFound:
        return (None, None) if when else None


def last_modified(headers):
    """The Last-Modified header as a Unix time, None if there is no usable one."""
    try:
        t = email.utils.parsedate_to_datetime(headers.get("Last-Modified") or "").timestamp()
    except (TypeError, ValueError, OverflowError, IndexError):
        return None
    return t if plausible_time(t, time.time()) else None


def list_objects(gcs, bucket, folder):
    """Every object under gs://bucket/folder, as TestGrid lists a build's artifacts:
    {name: (size, stored gzipped?, when GCS says it was written (None if not known))}."""
    out, token = {}, None
    for _ in range(MAX_LIST_PAGES):
        params = {"prefix": folder, "fields": "items(name,size,contentEncoding,timeCreated),nextPageToken"}
        if token:
            params["pageToken"] = token
        page = get_json(f"{gcs}/storage/v1/b/{quote(bucket)}/o?{urllib.parse.urlencode(params)}")
        items = page.get("items", []) if isinstance(page, dict) else None
        if not isinstance(items, list):
            raise IntegrityError(f"unexpected GCS listing for gs://{bucket}/{folder}")
        for item in items:
            name, size = (item.get("name"), item.get("size")) if isinstance(item, dict) else (None, None)
            if not isinstance(name, str) or not name.startswith(folder) or not isinstance(size, str) \
                    or not size.isascii() or not size.isdigit():
                raise IntegrityError(f"unexpected object in the GCS listing of gs://{bucket}/{folder}")
            out[name] = (int(size), str(item.get("contentEncoding") or "").strip().lower() == "gzip",
                         rfc3339_time(item.get("timeCreated")))
        token = page.get("nextPageToken")
        if not token:
            return out
    raise IntegrityError(f"the GCS listing of gs://{bucket}/{folder} did not end after {MAX_LIST_PAGES} pages")


def rfc3339_time(text):
    """A GCS timestamp ("2026-10-11T12:34:56.789Z") as a Unix time, None if it is not one."""
    try:
        t = dt.datetime.fromisoformat(text).timestamp() if isinstance(text, str) and len(text) <= 40 else None
    except (ValueError, OverflowError):
        return None
    return t if t is not None and plausible_time(t, time.time()) else None


DEADLINE = 24 * 3600  # TestGrid paints a build not finished this long after it started FAIL
POD_WAIT = 3600  # its Pod row waits this long after finished.json's timestamp for podinfo.json
UPDATE_SPAN = 20 * 60  # one update of a group takes at most this long (--group-timeout=20m), its stop set at the start
# TestGrid's updater hands each group out every 2 hours (--wait=2h; util/queue sets the next time
# as it hands one out), and drops a hand-out while an update of the group still runs: a column it
# reads again (within an update's 20 minutes) is read again within this long. An update appends the
# columns after its first only until its grace, half its 20 minutes (updater.go:572-580, 680-699):
# when its reads wait (for the result reader lock, or the pool), the others are read by the update
# that follows at once (unprocessed, updater.go:420-423), in its own 20 minutes. (An update that
# fails is tried again 30 to 60 minutes later, and one whose follow-ups are cut by the grace too
# reads the others an update later each, which TestGrid's records, not public, would show.)
READ_GAP = 2 * 3600 + 3 * UPDATE_SPAN
REPROCESS = 20 * 60  # an update that begins less than this long after a build started reads its column again
REPROCESS_DAYS = 7  # after its test group's config changed, it reads again its columns of builds started this recently
# TestGrid's components take a config this long after a run of the upload job uploaded it at
# most: its config merger copies it into the config they read every 15 minutes
# (cluster/prod/config_merger.yaml: --wait=15m), a merge taking up to 10 (cmd/config_merger),
# and they read that config every minute (config/snapshot; a failed read is tried again twice,
# 5 and 10 seconds later). (A merge that fails, such as when one of its sources cannot be
# read, keeps the config before until the next one: that cannot be seen from here.)
PICKUP = (15 + 10 + 1) * 60 + 30
# TestGrid's tabulator applies a tab's new filters when it next writes the tab: at its test
# group's next grid write (a column read), or within an hour (cluster/prod/tabulator.yaml:
# --wait=1h; a new config keeps each group's next time in its queue), so the old ones may
# show the columns read before this long more.
TABULATE = 3600
PENDING = BACKFILL_SLACK_HOURS * 3600  # a build may start this long after its ID was made
OLDER = PENDING + DEADLINE + READ_GAP + POD_WAIT + READ_GAP  # a build's column may run this long after its ID was made


def column_marks(probe, finished_at, podinfo_at, junits=()):
    """The times the column TestGrid reads of a build may change at (probe: the column as
    of now, for its start and finish times)."""
    marks = [finished_at, podinfo_at, *(j[3] for j in junits)]
    if probe.started:
        marks += [probe.started + DEADLINE, probe.started + DEADLINE - UPDATE_SPAN, probe.started + REPROCESS,
                  probe.started + REPROCESS + UPDATE_SPAN]
    if probe.finished:
        marks += [probe.finished + POD_WAIT, probe.finished + POD_WAIT - UPDATE_SPAN]
    return marks


def made_columns(column, at):
    """The columns an update may make of what GCS held at `at`: TestGrid compares a
    build's deadlines (24 hours to finish, an hour for podinfo.json) with the time it
    makes the column, after it read the build's files, up to UPDATE_SPAN later
    (convertResult, then time.Now()): column(at, clock) for both ends, one when alike."""
    first, later = column(at, at), column(at, at + UPDATE_SPAN)
    return (first,) if signature(first) == signature(later) else (first, later)


def runs(col):
    """Whether TestGrid reads the column again at its next update because of the column
    itself: a cell of it is RUNNING (the build, or its Pod row waiting for podinfo.json);
    a build without a start, until finished.json is in (then it never starts: its column
    stays where its read put it)."""
    if col.errors:
        return "RUNNING" in col.results and not getattr(col, "finished_seen", getattr(col, "has_finished", False))
    return "RUNNING" in (col.cells or {}).values()


def spans_union(spans):
    """The union of closed spans [(from, to)], as disjoint spans in order."""
    out = []
    for lo, hi in sorted(s for s in spans if s[0] <= s[1]):
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def spans_within(spans, lo, hi):
    """The parts of the spans within [lo, hi]."""
    return [(max(a, lo), min(b, hi)) for a, b in spans if max(a, lo) <= min(b, hi)]


def spans_minus(spans, cuts):
    """The parts of the spans outside the open cuts (from, to)."""
    out = []
    for piece in spans:
        pieces = [piece]
        for c_lo, c_hi in cuts:
            pieces = [p for a, b in pieces
                      for p in ([(a, b)] if c_hi <= a or c_lo >= b else [(a, c_lo)] * (a < c_lo) + [(c_hi, b)] * (c_hi < b))]
        out += pieces
    return spans_union(out)


def possible_reads(view, made, marks, holds, now, seeds=()):
    """When TestGrid may read a build's column, as its updater reads columns: first
    within READ_GAP after the build was made, then again within READ_GAP after each read
    at which the column runs (see runs()) or that falls in one of the spans `holds`
    gives as (from, to) (when an update reads it again for another reason: a column
    older than it runs in the grid then; it may be read at any time from `from` to a
    READ_GAP after `to`), and at any time in the spans `seeds` gives as (from, to) (an
    update that reads every column again, such as after its test group's config
    changed). view(t): the columns a read at t may make (see made_columns); marks: the
    times they may change at, so each moment and each stretch of time between marks
    reads alike. Returns (the reads up to now, per moment and stretch and column, as
    [(t, until, column)]: from t (the moment, or the start of the stretch) its tabs may
    show that column until the next read must replace it (within READ_GAP of the read
    when the column is read again; else inf); the last time a read may happen (inf:
    unbounded); the last time a read may find the column running). Each read is
    (t, until, column, end): `end` the last time it may happen."""
    seeds = [(max(lo, made), hi) for lo, hi in (*seeds, *((lo, hi + READ_GAP) for lo, hi in holds)) if hi >= made]
    marks = sorted({made, now, *(t for t in (*marks, *(x for span in (*holds, *seeds) for x in span))
                                 if t is not None and math.isfinite(t) and t > made)})
    reach, running_until, reads, last_cols = made + READ_GAP, -math.inf, [], ()
    last = max([reach, *(hi for _, hi in seeds)])
    for i, a in enumerate(marks):
        b = marks[i + 1] if i + 1 < len(marks) else math.inf
        for lo, hi in ((a, a), (a, b)):  # the moment a, then the stretch after it
            moment = hi == lo
            if lo > now or (lo == now and not moment):  # not read yet
                return reads, max(last, reach), math.inf if any(runs(c) for c in last_cols) else running_until
            seeded = any(s_lo <= lo <= s_hi if moment else s_lo < hi and lo < s_hi for s_lo, s_hi in seeds)
            if (lo > reach or (lo == reach and not moment)) and not seeded:  # no read can happen then
                continue
            cols = view(lo if moment else (lo + hi) / 2)
            cols = last_cols = cols if isinstance(cols, tuple) else (cols,)
            again_then = any(h_lo <= lo <= h_hi if moment else h_lo < hi and lo < h_hi for h_lo, h_hi in holds)
            running = any(runs(c) for c in cols)
            end = hi if moment or running or again_then else \
                min(hi, max([reach, *(s_hi for s_lo, s_hi in seeds if s_lo < hi and lo < s_hi)]))  # (when it may come)
            reads += [(lo, hi + READ_GAP if runs(c) or again_then else math.inf, c, end) for c in cols]
            if running:
                running_until = max(running_until, hi)
            if running or again_then:
                reach = max(reach, hi + READ_GAP)
    return reads, max(last, reach), running_until


def relevant_events(made, started, events):
    """Of `events` (see gcs_column), those after which an update may read again the column
    of a build made at `made` that started at `started` (None: not known; then PENDING
    after it was made at the latest): one that may begin after it was made, and, for a
    changed test group, less than REPROCESS_DAYS after it started."""
    recent = (made + PENDING if started is None else started) + REPROCESS_DAYS * 86400
    return [(lo, hi, how) for lo, hi, how in events if hi >= made and (how != "changed" or lo < recent)]


class BuildRuns:
    """When TestGrid's column of one of a job's builds may run (see Timeline), from its
    started.json, finished.json and podinfo.json as of the Timeline's now: .started (its
    start; None for a build without a usable one), .nostart (it has no start: TestGrid
    reads it as running), .good_until (from when a read finds its column errored, by a
    finished.json or podinfo.json that is not JSON: an errored column, like one without a
    start or one too old, does not set the start TestGrid puts a later column without one
    at; inf: never), .running_until (the last time a read may find it running; -inf:
    never; inf: it may still run now), .reach (the last time TestGrid may read it of its
    own accord: first, while it runs, and while it is new) and .runs_at(t) (whether the
    columns a read at t may make run: every one, or, with every=False, any)."""

    def __init__(self, timeline, build):
        self.build = build
        started, finished, finished_at, podinfo, podinfo_at = timeline.files(build)
        now, made, job = timeline.now, created(build), timeline.job
        floor = made - OLDER  # (any stop: only whether it is a start TestGrid takes matters here)
        parsed = {}  # (each file decoded once, for every column made of it)
        probe = tr.column(job, started, finished, podinfo, [], floor, now, parsed=parsed)
        self.nostart = "nostart" in probe.errors
        self.started = probe.started if probe.started and not {"ancient", "started"} & set(probe.errors) else None
        written = {"finished": finished_at, "podinfo": podinfo_at}  # (one without a time counts as there)
        self.good_until = min((-math.inf if written[e] is None else written[e] for e in probe.errors if e in written),
                              default=math.inf)

        def column(at, clock):
            held = lambda written: written is None or written <= at  # noqa: E731
            col = tr.column(job, started, finished if held(finished_at) else None,
                            podinfo if held(podinfo_at) else None, [], floor, clock, parsed=parsed,
                            options=timeline.options)
            col.finished_seen = finished is not None and held(finished_at)
            return col
        self._column = column
        _, self.reach, self.running_until = possible_reads(
            lambda at: made_columns(column, at), made, column_marks(probe, finished_at, podinfo_at), [], now,
            [(made, self.started + REPROCESS + UPDATE_SPAN)] if self.started else [])

    def runs_at(self, t, every=True):
        return (all if every else any)(runs(c) for c in made_columns(self._column, t))


def listed_again(ends, old):
    """When an update may list again, and read, a build whose column vanished (see
    HoldModel.vanishing: `ends`, each time a column may stop pinning it out): within READ_GAP
    after each; but by an update that begins then, not once the build is too old for it
    (`old`: its start and its tab's days of results): [(from, to)]."""
    return [(e, e + READ_GAP) for e in ends if e < old]


def order_key(started, build):
    """Where TestGrid's grid puts a column: by start, newest first; of the same start, the
    lower ID newer (the columns an update reads, in ID order, before the stable sort)."""
    return started, -int(build)


class HoldModel:
    """How a job's other builds (BuildRuns, `others`) make TestGrid's updater read a
    column again or drop it, as its grid holds their columns: an update re-reads every
    column from the oldest one that runs (truncateRunning), listing only the builds
    after the highest ID of the columns it keeps (hintStarted, listBuilds), and a build
    without a start is read as a running column put at the start of the good column read
    before it in the same update (readColumns; with none, at the epoch, where the next
    update drops it). span: the days of results of the tab, in seconds (a column that
    started before the update's stop is too old: it does not run, nor set that start);
    events: the updates after changes to the test group that read columns again (see
    gcs_column).

    Columns run, in the grid, from when the build was made until their last running read
    (.run_end), and the last read of the update that reads one done (READ_GAP later) is
    the last they hold: a column older than another holds it until then. A build without
    a start is put, by each update that reads it with good columns of lower IDs, at the
    start of the highest of them good then (as long as it is: until a file of it that is
    not JSON is written, and while it is not too old), and holds every column newer than
    that with a higher ID than its own until the next update: so each read of that good
    column (.reads: its own, those that columns older than it hold, and the update after
    a change that reads it again) is a hold of those columns (.entries). And a build
    whose column sorts newer than a column with a higher ID (it started later, or at the
    same time) vanishes from the grid once that column is settled there and nothing older
    than it runs, at the first update that cuts the grid at it or drops it as new (see
    vanishing()): it then no longer runs there, and is read again only by an update that
    lists it again, once no such column pins it out (that one too old, or cut at an older
    running column), or after a change that reads again all those columns (and then
    holds, while it runs, the columns newer than it with a higher ID than theirs until
    the next update). But an update appends the columns after its first only until its
    grace (updater.go:680-699), so when its reads come slowly that column may never be
    appended, and the build never vanish: the model reads and holds as it would then too
    (both: a vanished build is read, and holds, as if it never vanished, and once listed
    again; and pins nothing out)."""

    def __init__(self, others, span, events=()):
        self.span, self.events = span, events
        self.by_id = sorted(others, key=lambda r: int(r.build))
        self.good = sorted((r for r in others if r.started), key=lambda r: order_key(r.started, r.build))
        self.gone, self.out = {}, None
        self._pass()
        # (a build vanishes only past a column of a higher ID that sorts older: none in most jobs)
        bad, top = [], -1
        for x in self.good:  # (oldest first)
            if top > int(x.build):
                bad.append(x)
            top = max(top, int(x.build))
        # (each from the same model, so the order they are taken in does not matter; then again from the
        # model with them gone, while that changes them, VANISH_ROUNDS times at most)
        for _ in range(VANISH_ROUNDS):
            gone = {}
            for x in bad:
                v = self.vanishing(x.build, x.started)
                if v is not None and x.runs_at(v[1]):  # (it runs at every read until it vanishes)
                    gone[x.build] = v
            if gone == self.gone:
                break
            self.gone = gone
            self._pass()

    def vanish(self, build, started):
        """vanishing() of a build, as the model took it (for one that vanishes, what it found last)."""
        return self.gone[build] if build in self.gone else self.vanishing(build, started)

    def run_end(self, r):
        """The last time a read may find r's column running in the grid (one that vanishes may
        also never vanish; but not in the world `out` is of, see vanishing())."""
        end = min(r.running_until, r.started + self.span + UPDATE_SPAN)  # (a column too old does not run)
        return min(end, self.gone[r.build][1]) if r.build == self.out else end

    def held_by(self, end, at):
        """The end of a hold by a running column put at `at` (a start) whose last running read
        may happen by `end`: the update after it reads the columns newer than it again, within
        READ_GAP, if it begins before that column is too old; one that begins later drops it
        first (InflateGrid), and reads nothing again."""
        return min(end, at + self.span + UPDATE_SPAN - READ_GAP)

    def _pass(self):
        span, gone = self.span, self.gone
        self.keys = [order_key(r.started, r.build) for r in self.good]
        self.ran = list(itertools.accumulate((self.held_by(self.run_end(r), r.started) for r in self.good), max))
        # (from, to, the start it is put at, the ID it holds columns above): a running column that holds
        # the columns that started after that start and have a higher ID, from `from` to `to`
        self.entries = []
        self.reads = {}  # per good build: when a read may find its column good
        below = []  # the good builds so far, in ID order
        for r in self.by_id:
            made = created(r.build)
            if r.started:
                held = self.hold_spans(made, r.started, r.build)
                spans = [(made, r.reach), *((lo, hi + READ_GAP) for lo, hi in held)]
                recent = r.started + REPROCESS_DAYS * 86400
                spans += [(lo, (hi if how != "changed" else min(hi, recent)) + UPDATE_SPAN) for lo, hi, how in self.events
                          if hi >= made and (how != "changed" or lo < recent)]
                if r.build in gone:  # (read as if it never vanished, and again once listed again)
                    _, until, _, back, ends = gone[r.build]
                    spans = spans_within(spans, -math.inf, until) if r.build == self.out else \
                        spans + listed_again(ends, r.started + span)
                    # (listed again, running: it holds the newer columns until the next update, those of
                    # a lower ID too, as the update lists every build after the highest ID kept, now lower)
                    if back < math.inf and r.runs_at(back, every=False):
                        self.entries.append((back, max(back, self.held_by(r.running_until, r.started)), r.started, -1))
                cap = min(r.good_until, r.started + span + UPDATE_SPAN)
                self.reads[r.build] = spans_union(spans_within(spans, -math.inf, cap))
                below.append(r)
            elif r.nostart:
                start = made  # (from when it is listed)
                for g in reversed(below):  # (the highest good one below it is the one it is put at, while it is good)
                    cap = min(g.good_until, g.started + span + UPDATE_SPAN)
                    if cap <= start:
                        continue
                    # (a read of it less than a gap before it is too old holds them too, if the next
                    # update begins in time: from that read on)
                    self.entries += [(lo, max(lo, min(hi, self.held_by(cap, g.started))), g.started, int(r.build))
                                     for lo, hi in spans_within(self.reads[g.build], start, cap)]
                    start = cap
                    if start == math.inf:
                        break

    def hold_spans(self, made, started, build):
        """When the grid may hold a running column older than a build's (made at `made`, that
        started at `started`, ID `build`), from when it was made: [(from, to)]."""
        bid = int(build)
        i = bisect.bisect_left(self.keys, order_key(started, build))
        end = self.ran[i - 1] if i else -math.inf
        out = [(made, end)] if end >= made else []
        out += [(max(lo, made), hi) for lo, hi, at, above in self.entries if above < bid and at < started and hi >= made]
        return spans_union(out)

    def holds(self, build, started):
        """hold_spans() of a build, or for one without a start (None): any column of a lower ID
        that runs, or one without a start of a lower ID put at a good start."""
        if started is not None:
            return self.hold_spans(created(build), started, build)
        made, bid = created(build), int(build)
        end = max((self.held_by(self.run_end(r), r.started) for r in self.good if int(r.build) < bid), default=-math.inf)
        return spans_union(([(made, end)] if end >= made else []) +
                           [(max(lo, made), hi) for lo, hi, _, above in self.entries if above < bid and hi >= made])

    def vanishing(self, build, started):
        """(done, gone, pinned, back, ends) when the build's column vanishes from the grid if it runs
        at every read from `done` to `gone` (else None). A column of a higher ID that sorts older than it
        (it started earlier, or at the same time) pins it out of the listing while it is settled
        in the grid: from when its update that reads it done (or first) is over; while it is
        good (read errored, it is put at the start of the good column read before it, not its
        own) and not too old (the updates after that drop it); and but while a column older
        than it runs (the grid is cut there, and the build listed again) or an update after a
        change reads it again: an update then lists only the builds after it (truncateRunning,
        then listBuilds from the highest hint of the columns kept). pinned: [(from, to)]: when
        one pins it, not read then. The first update in them cuts the grid at its running
        column, and drops it: the update before reads it for the last time, by `gone`. Out of
        the grid, a column of a higher ID pins it out so whatever its start: `back` is when none
        does any more (inf: never), and the update after lists it again, and reads it; while it
        then runs in the grid, only one that sorts older pins it out. Until then the build holds
        no column, nor is one put at its start: the columns that sort newer pin it out as they do
        in the grid without it from `gone` on (not as its own reads after that, which come only
        once it is listed again, would hold them). A column that vanishes itself may also never
        vanish (an update appends the columns after its first only until its grace): `pinned`
        and `gone` are of the world where each pins it out, `back` of the one where none does,
        and `ends` every time it may be listed again: `back`, and the end of each column's pin
        after it (see listed_again)."""
        bid, key = int(build), order_key(started, build)

        def pins(model, c, anyway=False):  # (when c, of a higher ID, is settled in the grid, and kept by every update)
            if c.build in model.gone and not anyway or c.running_until == math.inf:
                return []  # (it vanishes itself, or may run on: not settled in the grid, or not yet)
            made = created(c.build)
            settled = max(max(model.run_end(c), made) + READ_GAP, c.started + REPROCESS + UPDATE_SPAN)
            if c.good_until <= settled:
                return []  # (read done, it is errored: put at the start of the good column read before it)
            recent = c.started + REPROCESS_DAYS * 86400
            cut = [(lo, hi + READ_GAP) for lo, hi in model.hold_spans(made, c.started, c.build)]
            cut += [(lo, (hi if how != "changed" else min(hi, recent)) + UPDATE_SPAN) for lo, hi, how in self.events
                    if hi >= made and (how != "changed" or lo < recent)]
            return spans_minus([(settled, c.started + self.span)], cut)
        higher = [c for c in self.good if int(c.build) > bid]
        older = [c for c in higher if order_key(c.started, c.build) < key]
        newer = [c for c in higher if order_key(c.started, c.build) > key]
        # (a column that vanishes itself pins it out in the world where it never vanishes, not in the other)
        pinned = spans_union([p for c in older for p in pins(self, c, anyway=True)])
        if not pinned:
            return None
        gone = pinned[0][0]
        without = copy.copy(self)  # (the model with the build gone from `gone` on, never listed again)
        without.gone, without.out = {**self.gone, build: (gone - READ_GAP, gone, pinned, math.inf, [])}, build
        if newer:
            without._pass()
        later = [(pins(self, c, anyway=True), pins(self, c)) for c in older] + \
            [(spans_within(pins(without, c, anyway=True), gone, math.inf), spans_within(pins(without, c), gone, math.inf))
             for c in newer]
        # (listed again at the earliest once no column but one that vanishes itself pins it out; and, as those
        # may not vanish, at the end of any column's pin after that, but while one that does not pins it out)
        least = spans_union([p for _, some in later for p in some])
        back = next((hi for lo, hi in least if lo <= gone <= hi), gone)
        ends = sorted({back} | {hi for every, _ in later for _, hi in every
                               if hi > back and not any(lo < hi < end for lo, end in least)})
        return gone - READ_GAP, gone, pinned, back, ends


class Timeline:
    """A job's other builds, as far as TestGrid's reads of one of them depend on them (see
    HoldModel). Each build's started.json, finished.json and podinfo.json are read once,
    as of the Timeline's `now`, so one Timeline serves every build of the job a run
    judges (a build's own files are read by gcs_column, as of when it judges it)."""

    def __init__(self, gcs, query, now=None, options=None, shared=None):
        self.gcs, self.query, self.now = gcs, query, time.time() if now is None else now
        self.options = options or tr.DEFAULT_OPTIONS  # (its test group's cell options: a column runs by them)
        self.job = query.rsplit("/", 1)[-1]
        self._lock = threading.Lock()
        shared = shared if shared is not None else Timelines.shared()
        self._listing, self._list_lock = shared["listing"], shared["list_lock"]  # (one for every options)
        self._files, self._files_lock = shared["files"], shared["lock"]  # build -> Future of its files
        self._runs = {}  # build -> Future of its BuildRuns
        self._models = {}  # (days of results, events) -> (from, to, the HoldModel of the builds made then)
        self._model_lock = threading.Lock()  # (one made at a time: they take CPU, and a later build may reuse it)

    def files(self, build):
        """A build's started.json, finished.json (with when it was written) and podinfo.json
        (with when), read once for every Timeline of the job."""
        with self._files_lock:
            fut = self._files.get(build)
            mine = fut is None
            if mine:
                fut = self._files[build] = cf.Future()
        if mine:
            try:
                base = f"{self.gcs}/{build_folder(self.gcs, self.query, build)}"
                started = read_artifact(f"{base}/started.json", MAX_JSON_BYTES)
                finished, finished_at = read_artifact(f"{base}/finished.json", MAX_JSON_BYTES, when=True)
                podinfo, podinfo_at = read_artifact(f"{base}/podinfo.json", MAX_JSON_BYTES, when=True)
                fut.set_result((started, finished, finished_at, podinfo, podinfo_at))
            except BaseException as e:
                fut.set_exception(e)
        return fut.result()

    def offer(self, build, files):
        """Take a build's files (as files() returns them) read elsewhere, unless they were read."""
        with self._files_lock:
            if build not in self._files:
                fut = self._files[build] = cf.Future()
                fut.set_result(files)

    def builds(self, lo, hi):
        """The job's build IDs made in [lo, hi]: what of that was not listed yet is listed
        (once for every Timeline of the job), then kept."""
        with self._list_lock:
            have = self._listing  # {"lo", "hi", "ids"}: every build made in [lo, hi]
            if have["ids"] is None or lo < have["lo"] or hi > have["hi"]:
                parts = [(lo, hi)] if have["ids"] is None else \
                    [p for p in ((lo, have["lo"]), (have["hi"], hi)) if p[0] < p[1]]
                got = set(have["ids"] or ())
                for a, b in parts:
                    try:
                        got.update(list_build_ids(self.gcs, self.query, a, b))
                    except NotFound:  # no such bucket: TestGrid lists no builds either (storage.ErrBucketNotExist)
                        pass
                first = have["ids"] is None
                have.update(ids=sorted(got, key=int), lo=lo if first else min(lo, have["lo"]),
                            hi=hi if first else max(hi, have["hi"]))
            return [b for b in have["ids"] if lo <= created(b) <= hi]

    def listed(self):
        """[from, to]: when the builds listed so far were made (an empty span if none were)."""
        with self._list_lock:
            return self._listing["lo"], self._listing["hi"]

    def runs(self, build):
        """The build's BuildRuns, read once."""
        with self._lock:
            fut = self._runs.get(build)
            mine = fut is None
            if mine:
                fut = self._runs[build] = cf.Future()
        if mine:
            try:
                fut.set_result(BuildRuns(self, build))
            except BaseException as e:
                fut.set_exception(e)
        return fut.result()

    def read(self, build):
        """The build's BuildRuns if it was read, else None (also if reading it failed)."""
        with self._lock:
            fut = self._runs.get(build)
        return fut.result() if fut is not None and fut.done() and fut.exception() is None else None

    @staticmethod
    def back(made, events=(), span=tr.DAYS_OF_RESULTS * 86400, nostart=0, started=None):
        """How long before a build made at `made` (that started at `started`; None: not known
        yet) the builds TestGrid's reads of it depend on were made, at most: OLDER, a READ_GAP
        more for each build without a start among them (the chains they make), and, for each
        of `events` (see gcs_column) that may read it again, or start a chain of holds that
        reaches it, back to the builds that update may read again (it puts every build without
        a start it reads at the start of a good column, and the updates after it read the
        newer columns again as they take them off, one an update: a READ_GAP for each, and one
        more); MAX_LOOKBACK at most."""
        back = OLDER + READ_GAP * nostart
        recent = (made + PENDING if started is None else started) + REPROCESS_DAYS * 86400
        reach = READ_GAP * (nostart + 1) + UPDATE_SPAN
        for lo, hi, how in events:
            if hi + reach >= made and (how != "changed" or lo < recent):
                back = max(back, made - lo + (span if how != "changed" else REPROCESS_DAYS * 86400) + PENDING)
        return min(back, Timeline.most(span))

    @staticmethod
    def most(span):
        """How far back a window reaches at most: MAX_LOOKBACK, or, on a tab of more days of
        results, those and the week an update after a change reads again."""
        return max(MAX_LOOKBACK, span + REPROCESS_DAYS * 86400 + PENDING)

    def window(self, made, events=(), span=tr.DAYS_OF_RESULTS * 86400, started=None, exclude=None):
        """(from, to): when the builds TestGrid's reads of a build made at `made` depend on
        were made: back() before it, counting the builds without a start in that window (but
        `exclude`, the build itself), to PENDING after it."""
        nostart = 0
        while True:
            lo, hi = made - self.back(made, events, span, nostart, started), made + PENDING
            n = sum(1 for b in self.builds(lo, hi) if b != exclude and self.runs(b).nostart)
            if self.back(made, events, span, n, started) <= made - lo:
                return lo, hi
            nostart = n

    def grid(self, build, started, days=tr.DAYS_OF_RESULTS, events=()):
        """How the job's other builds make TestGrid read the build's column again, or drop it:
        (holds, vanish), as HoldModel's holds() and vanishing() (vanish None for a build without
        a start); holds [(made, inf)] (and vanish None) when the job's builds cannot be read.
        events: the updates after changes to its test group (see gcs_column), all of them (one
        before the build was made may start a chain of holds that reaches it). One HoldModel
        serves every build of the job judged with the same days and events whose window it
        holds (the builds outside a build's window change nothing for it)."""
        made, span = created(build), days * 86400
        key = (span, tuple(sorted(events)))
        try:
            lo, hi = self.window(made, events, span, started, build)
            if made - lo >= self.most(span):  # (a window cut short: the builds of its own, whatever was judged before)
                have = (lo, hi, HoldModel([self.runs(b) for b in self.builds(lo, hi)], span, events))
            else:
                with self._model_lock:
                    have = self._models.get(key)
                    if have is None or lo < have[0] or hi > have[1]:
                        if have is not None:
                            lo, hi = min(lo, have[0]), max(hi, have[1])
                        have = self._models[key] = (lo, hi, HoldModel([self.runs(b) for b in self.builds(lo, hi)],
                                                                      span, events))
        except (urllib.error.HTTPError, *TRANSIENT):
            return [(made, math.inf)], None
        model = have[2]
        return model.holds(build, started), None if started is None else model.vanish(build, started)

    def prime(self, pool, made):
        """Start reading, in the pool, the builds window() reads for builds made at `made`
        without events (but those without a start among them); returns the reads' futures."""
        try:
            ids = self.builds(min(made) - OLDER, max(made) + PENDING)
        except (urllib.error.HTTPError, *TRANSIENT):
            return []  # (window() lists them again)
        return [pool.submit(self.runs, b) for b in ids]


class Timelines:
    """One run's Timeline of each job and cell options (see gcs_column), made as they are
    needed; the Timelines of a job share its listings and its builds' files."""

    def __init__(self, now):
        self.now, self._lock, self._of, self._shared = now, threading.Lock(), {}, {}

    @staticmethod
    def shared():
        return {"listing": {"lo": math.inf, "hi": -math.inf, "ids": None}, "list_lock": threading.Lock(),
                "files": {}, "lock": threading.Lock()}

    def get(self, gcs, query, options=None):
        options = options or tr.DEFAULT_OPTIONS
        with self._lock:
            if (gcs, query, options) not in self._of:
                shared = self._shared.setdefault((gcs, query), self.shared())
                self._of[(gcs, query, options)] = Timeline(gcs, query, self.now, options, shared)
            return self._of[(gcs, query, options)]

    def prime(self, pool, gcs, builds, deadline=math.inf, events=None, days=None):
        """Read, in the pool, what judging these (query, build)s will need (waiting for it until
        `deadline`, a time.monotonic()): the builds of each one's Timeline window (with its
        events, {(query, build): events}, and days of results, {(query, build): days}), each
        job's listed at once, then all their files read at once, then those of what the
        builds without a start among them widen the windows by, until none does."""
        events, days = events or {}, days or {}
        want = {}  # query -> [[made, its events, days of results (s), builds without a start in its window]]
        for query, build in builds:
            want.setdefault(query, []).append([created(build), events.get((query, build), ()),
                                               days.get((query, build), tr.DAYS_OF_RESULTS) * 86400, 0])

        def left():
            return None if deadline == math.inf else max(0, deadline - time.monotonic())
        while want and left() != 0:
            listed = {q: pool.submit(self.get(gcs, q).builds,
                                     min(m - Timeline.back(m, ev, sp, n) for m, ev, sp, n in items),
                                     max(m for m, _, _, _ in items) + PENDING)
                      for q, items in sorted(want.items())}
            cf.wait(listed.values(), timeout=left())
            ids = {q: fut.result() for q, fut in listed.items() if fut.done() and fut.exception() is None}
            reads = [pool.submit(self.get(gcs, q).runs, b) for q, bs in ids.items() for b in bs
                     if self.get(gcs, q).read(b) is None]
            cf.wait(reads, timeout=left())
            wider = {}
            for q, bs in ids.items():
                timeline = self.get(gcs, q)
                runs = [timeline.read(b) for b in bs]
                if None in runs:
                    continue  # (out of time, or a build that cannot be read: window() reads it again)
                nostart = [created(r.build) for r in runs if r.nostart]
                for item in want[q]:
                    m, ev, sp, n = item
                    count = sum(1 for t in nostart if m - Timeline.back(m, ev, sp, n) <= t <= m + PENDING)
                    if Timeline.back(m, ev, sp, count) > Timeline.back(m, ev, sp, n):
                        item[3] = count
                        wider[q] = want[q]
            want = wider


def gcs_column(gcs, query, build, days=tr.DAYS_OF_RESULTS, now=None, timeline=None, events=(), options=None,
               inputs=None):
    """TestGrid's column for a build, from GCS read as its updater reads it (see
    testgrid_rule): started.json, finished.json, podinfo.json and, unless reading
    those failed (that error is then the column), every junit file it would parse.
    Reads of those three files that fail raise; if the junit files cannot be listed
    or read, the column is the one without them, with .partial saying why (see
    verdict).

    TestGrid reads a build's column again and again (see possible_reads): while it
    runs, while the build started less than REPROCESS before an update, while the grid
    holds an older running column of its job (see HoldModel: `timeline`, else one for
    this build), and at the update after its test group's config changed, if it started
    less than REPROCESS_DAYS before (or every build, for a new group; `events`: when
    those updates may begin, as (from, to, "changed", "new", or "unknown": taken as new)), each time from what GCS
    holds then (objects by when GCS says they were written; one without a time counts as
    there), and makes the column up to UPDATE_SPAN later (see made_columns). A column
    stays on its tabs until the next read of it (one that may vanish from the grid, see
    HoldModel.vanishing, is read as if it never vanished too, and once listed again), so the
    build was red on a tab if the column of any read is.
    The column returned is the one of the last read up to now; .reads: each column read,
    the last first, with when it may have been read and its tabs may have shown it
    ([(column, [(from, to, read by)])]: from `from` until `to`, read by `read by` at the
    latest); .alternatives the other columns read; .open says whether TestGrid may read
    it again after now. A build counts as too old by the days of results (`days`) before
    the update that read it began. options: the test group's cell options
    (testgrid_rule.Options); inputs: a dict that keeps what GCS holds of the build for
    calls with other options (see Columns). .inputs: what GCS holds, as given to the
    rule."""
    now = time.time() if now is None else now
    job = query.rsplit("/", 1)[-1]
    made, span = created(build), int(days * 86400)
    if inputs is None or not inputs:
        read = read_build(gcs, query, build, made - span, now)
        if inputs is not None:
            inputs.update(read)
    else:
        read = inputs
    timeline = timeline or Timeline(gcs, query, now, options)
    timeline.offer(build, tuple(read[k] for k in ("started", "finished", "finished_at", "podinfo", "podinfo_at")))
    started, finished, finished_at, podinfo, podinfo_at, probe, s, junits, folder, partial, parsed = (read[k] for k in (
        "started", "finished", "finished_at", "podinfo", "podinfo_at", "probe", "s", "junits", "folder", "partial",
        "parsed"))
    options = options or tr.DEFAULT_OPTIONS

    def column(at, clock):
        # (made at `clock` by an update that read the build at `at`: one that began UPDATE_SPAN before
        # `clock` at the earliest, and so took its stop then)
        held = lambda written: written is None or written <= at  # noqa: E731
        col = tr.column(job, started, finished if held(finished_at) else None, podinfo if held(podinfo_at) else None,
                        [(j[0], j[1], j[2], j[4]) for j in junits if held(j[3])], clock - UPDATE_SPAN - span, clock,
                        folder=folder, parsed=parsed, options=options)
        col.finished_seen = finished is not None and held(finished_at)
        return col

    def view(at):
        return made_columns(column, at)

    changes = relevant_events(made, s, events)
    holds, vanish = timeline.grid(build, s, days, events)
    marks = column_marks(probe, finished_at, podinfo_at, junits) + ([s + span + UPDATE_SPAN] if s else [])  # (too old)
    # (after a change to its test group, an update that begins later than `recent` does not read it
    # again; the first update of a new group reads every build it shows)
    recent = (s or made) + REPROCESS_DAYS * 86400
    seeds = [(lo, (hi if how != "changed" else min(hi, recent)) + UPDATE_SPAN) for lo, hi, how in changes]
    if s:  # (an update that begins less than REPROCESS after it started reads it again, within its span)
        seeds.append((made, s + REPROCESS + UPDATE_SPAN))
    if vanish:  # (once it vanished, it is read again once listed again)
        done, gone, pinned, back, ends = vanish
        marks += [done, gone, *ends, *(x for w in pinned for x in w)]
    reads, reach, _ = possible_reads(view, made, marks, holds, now, seeds)
    if vanish:  # (it vanishes from the grid if every read of it from `done` to `gone` finds it running)
        late = [c for t, _, c, _ in reads if done <= t <= gone]
        if late and all(runs(c) for c in late):
            # (its reads as if it never vanished stay: the column that pins it out may never be appended,
            # as an update appends the columns after its first only until its grace; HoldModel.vanishing)
            again = listed_again(ends, s + span)
            if again:
                reads, reach, _ = possible_reads(view, made, marks, holds, now, seeds + again)
    if not reads:  # (an ID made after now)
        reads = [(now, math.inf, c, now) for c in view(now)]
    col = reads[-1][2]
    shown = {}  # per column read: when it may have been read, and its tabs may have shown it (the last read first)
    for lo, until, c, end in reversed(reads):
        shown.setdefault(signature(c), (c, []))[1].append((lo, until, end))
    for c, _ in shown.values():
        c.has_finished, c.partial = finished is not None, partial
    col.reads = list(shown.values())
    col.alternatives = [c for c, _ in col.reads[1:]]
    # (after the build is too old for TestGrid to show, a read of it cannot matter)
    col.open, col.now = min(reach, s + span + UPDATE_SPAN if s else math.inf) > now, now
    col.inputs = (started, finished, podinfo, [j[:3] for j in junits])
    return col


def read_build(gcs, query, build, stop, now):
    """What gcs_column reads of a build from GCS (once for every cell options it is judged
    by): its started.json, finished.json and podinfo.json with when they were written, the
    column without junit files as of now (a stop of `stop`, for its start), and, for a
    build with a start TestGrid takes, the junit files it reads, as (object it opens,
    size, data, when written, name as listed), or why they cannot be read."""
    where = build_folder(gcs, query, build)
    base = f"{gcs}/{where}"
    job = query.rsplit("/", 1)[-1]
    started = read_artifact(f"{base}/started.json", MAX_JSON_BYTES)
    finished, finished_at = read_artifact(f"{base}/finished.json", MAX_JSON_BYTES, when=True)
    podinfo, podinfo_at = read_artifact(f"{base}/podinfo.json", MAX_JSON_BYTES, when=True)
    probe = tr.column(job, started, finished, podinfo, [], stop, now)
    s = probe.started if probe.started and not {"ancient", "started"} & set(probe.errors) else None
    partial, junits, folder = None, [], ""
    if s:  # (TestGrid lists the junit files of a build it can read: one with a start it takes)
        bucket, _, path = where.partition("/")
        folder = f"{path}/"
        try:
            listing = list_objects(gcs, bucket, folder)
            for listed, opened in tr.junit_reads(sorted(listing)):
                size, gzipped, written = listing.get(opened, (None, False, None))
                if size is None:
                    junits.append((opened, None, None, None, listed))  # TestGrid cannot open it either
                    continue
                size = -1 if gzipped else size  # read decompressed, of unknown length
                data = b"" if size > tr.JUNIT_MAX_SIZE else read_artifact(
                    f"{gcs}/{bucket}/{urllib.parse.quote(opened, safe='/')}", MAX_JUNIT_BYTES)
                junits.append((opened, size, data, written, listed))
        except (urllib.error.HTTPError, *TRANSIENT) as e:  # TestGrid read them then; they cannot be read now
            partial = f"its junit files cannot be read: {type(e).__name__}: {e}"[:300]
            junits = []
    return {"started": started, "finished": finished, "finished_at": finished_at, "podinfo": podinfo,
            "podinfo_at": podinfo_at, "probe": probe, "s": s, "junits": junits, "folder": folder, "partial": partial,
            "parsed": {}}


class Columns:
    """A build's columns (see gcs_column), one per cell options its tabs' configs give,
    made as they are asked for, from one read of the build: columns(options)."""

    def __init__(self, gcs, query, build, days, now=None, timelines=None, events=()):
        self.args, self.timelines, self.events = (gcs, query, build, days, now), timelines, events
        self.inputs, self.made, self.lock = {}, {}, threading.Lock()

    def __call__(self, options=None):
        options = options or tr.DEFAULT_OPTIONS
        with self.lock:  # (one at a time: they share the build's read)
            if options not in self.made:
                gcs, query, build, days, now = self.args
                self.made[options] = gcs_column(
                    gcs, query, build, days, now=now,
                    timeline=self.timelines.get(gcs, query, options) if self.timelines else None,
                    events=self.events, options=options, inputs=self.inputs)
            return self.made[options]


def signature(col):
    """What tells two columns read of one build apart."""
    return (tuple(col.errors), tuple(col.malformed), tuple(sorted(col.results)),
            tuple(sorted((col.cells or {}).items())), tuple(sorted(col.notes.items())))


def unreadable_junit(col):
    """Why the column cannot be judged though nothing else keeps it undecided: its junit
    files could not be read, and TestGrid will not read it again. None otherwise."""
    return col.partial if getattr(col, "partial", None) and not still_read(col) else None


def still_read(col):
    """Whether TestGrid may read the column again (.open; a column gcs_column did not read:
    whether it runs)."""
    still = getattr(col, "open", None)
    return runs(col) if still is None else still


def verdict(col, job, rows=None, shows=None, during=None, read_in=None):
    """What TestGrid's column for the build shows on a tab: ("red", red rows) when the
    column of any read of it (col and col.alternatives, see gcs_column; and of the
    columns readResult's racing reads can give) is red there: the red rows of the last
    read (col) when it is red, as TestGrid shows them from then on, else those of the
    others; else ("undecided", []) while TestGrid may still read it (see still_read) or
    its junit files cannot be read, else ("not-red", []). shows(row): whether the tab
    shows a row (its config's filters, see testgrid_config.TabRule); without it, rows:
    {"overall": shown?, "pod": shown?}, as learned from the tab's table (shown when not
    known), and every other row is shown. during: (from, to): only the reads the tab may
    have shown then count (see gcs_column's .reads), and one TestGrid may make after now
    only if `to` is later; read_in: (from, to): only the reads TestGrid may have made then
    (with the cell options of then, which col's are)."""
    reads = getattr(col, "reads", None)
    if reads is None:  # (a column gcs_column did not read: all it read was shown at any time)
        reads = [(c, [(-math.inf, math.inf, math.inf)]) for c in (col, *getattr(col, "alternatives", ()))]
    chosen = [c for c, windows in reads
              if any((during is None or (a <= during[1] and during[0] <= b)) and
                     (read_in is None or (a <= read_in[1] and read_in[0] <= end)) for a, b, end in windows)]
    verdicts = [column_verdict(c, job, rows, shows) for c in chosen]
    if verdicts and chosen[0] is col and verdicts[0][0] == "red":
        return verdicts[0]
    state, cells = combine(verdicts)
    if state == "red":
        return state, cells
    later = (during is None or during[1] > getattr(col, "now", -math.inf)) and \
        (read_in is None or read_in[1] > getattr(col, "now", -math.inf))
    return ("undecided" if still_read(col) and later or getattr(col, "partial", None) else "not-red"), []


def shown_during(col, lo, hi):
    """Whether a tab may have shown a read of the build's column at some time in [lo, hi]
    (see gcs_column's .reads), or may show one TestGrid reads after now."""
    reads = getattr(col, "reads", None)
    if reads is None:
        return True
    return any(a <= hi and lo <= b for _, windows in reads for a, b, _ in windows) \
        or (still_read(col) and hi > getattr(col, "now", -math.inf))


def combine(verdicts):
    """One verdict from several: red with every red row when any is red, else undecided when any is."""
    red = {}
    for state, cells in verdicts:
        for c in cells if state == "red" else ():
            red.setdefault(c["test"], c)
    if red:
        return "red", list(red.values())
    return ("undecided" if any(state == "undecided" for state, _ in verdicts) else "not-red"), []


def view_end(made, days):
    """The last time TestGrid may show a build made then on a tab of `days` days of results
    (it started at most PENDING after it was made; an update may take UPDATE_SPAN)."""
    return made + PENDING + days * 86400 + UPDATE_SPAN


class Elsewhere:
    """A tab's config whose test group read another job's builds than the one judged (see
    tab_eras): .rule, its TabRule."""

    def __init__(self, rule):
        self.rule = rule


def tab_eras(rule, made, query=None):
    """The tab's configs while TestGrid may have shown a build made then (its rule's
    "eras", see ConfigHistory.eras; each by its own days of results), as [(from, to,
    config)]: a TabRule, UNKNOWN for a config the archive does not know, None while
    TestGrid had no such tab, or (for a build of `query`) Elsewhere while its test group
    read another job's builds (unless the tab's table disagreed with its config then: see
    run()'s "unreliable_until"); None for a rule without them."""
    eras = rule.get("eras")
    if eras is None:
        return None
    days = rule.get("days", tr.DAYS_OF_RESULTS)
    other = query is not None and made >= rule.get("unreliable_until", -math.inf)
    return [(lo, hi, Elsewhere(r) if other and isinstance(r, tgc.TabRule) and r.prefix is not None
             and r.prefix != query else r) for lo, hi, r in eras
            if hi >= made and lo <= view_end(made, r.days if isinstance(r, tgc.TabRule) else days)]


def build_days(rule, made):
    """The days of results TestGrid read a build made then with on the tab: the most of
    its configs while it may have shown it (see tab_eras), else the tab's."""
    days = [r.days for _, _, r in tab_eras(rule, made) or () if isinstance(r, tgc.TabRule)]
    return max(days) if days else rule.get("days", tr.DAYS_OF_RESULTS)


def tab_events(tab_rules, keys):
    """When updates may begin that read a build's column on these tabs again (their
    test groups' config changed: see ConfigHistory.events)."""
    return sorted({e for k in keys for e in (tab_rules.get(k) or {}).get("events", ())})


def tab_verdict(cols, job, rule, made, query=None, since=None):
    """verdict() on a tab by its rule (see run()): by each config TestGrid had for the
    tab while it may have shown the build ("eras"; else the one known; one whose test
    group read another job than the build's, `query` (by default the Columns'), as if
    TestGrid had no such tab then), each on the reads the tab may have shown while that
    config was in force (and from `since` on, if given); each read's column made with
    the cell options of the config in force when TestGrid may have made it
    (cols(options), see Columns; or one column, made with the tab's options), and
    undecided where those are options the port does not follow; red when it is red by
    one, undecided while one is not known (see config_verdict)."""
    col_of = cols if isinstance(cols, Columns) else lambda options: cols
    if query is None and isinstance(cols, Columns):
        query = cols.args[1]
    eras = tab_eras(rule, made, query)
    if eras is None:
        return config_verdict(col_of(rule.get("options")), job, rule, made,
                              None if since is None else (since, math.inf))
    # The reads TestGrid made under each config, by its cell options (from its run's start to
    # an update after the next one's PICKUP, which may begin then and read for its span; none
    # while TestGrid did not update the group, or while it read another job's builds, nor, once
    # it reads this job again, of a build made before that it does not list again: one older
    # than the REPROCESS_DAYS the update after the change reads again; while TestGrid had no
    # such tab, its group's options are not known: by each).
    made_with, away = [], None  # (away: when the group began reading another job, while it does)
    for lo, hi, r in eras:
        if isinstance(r, Elsewhere):
            away = lo if away is None else away
            continue
        back, away = away is not None, None
        if isinstance(r, tgc.TabRule) and not r.not_updated and \
                not (back and made + PENDING < lo - REPROCESS_DAYS * 86400):
            made_with.append((r.options, (lo, hi + UPDATE_SPAN), r.group_problem))
    made_with += [(options, (lo, hi + UPDATE_SPAN), problem) for lo, hi, r in eras if r is None
                  for options, problem in dict.fromkeys((o, p) for o, _, p in made_with)]
    # (and while a config not known yet was in force, by options not known: undecided if shown)
    made_with += [(rule.get("options"), (lo, hi + UPDATE_SPAN), "its options then are not known yet")
                  for lo, hi, r in eras if r is UNKNOWN]
    verdicts, away = [], None
    for lo, hi, r in eras:
        # (none: TestGrid had no such tab then; broken: its tabulator could not apply the tab's
        # filters, and the tab showed the columns it had, by the config before (that era's); not
        # updated: TestGrid read no build of the group then, but the tab showed the grid it had)
        elsewhere = isinstance(r, Elsewhere)
        away = (lo if away is None else away) if elsewhere else None
        if elsewhere:
            # (its group reads another job: it keeps the columns of this one, until they are too old,
            # and reads none of them again; TestGrid does not read a changed group's columns again, no
            # grid it stores holding the config reprocessColumn compares, updater.go:718-735, 765-767)
            if made >= away:
                continue
            r, made_by = r.rule, away
        else:
            made_by = math.inf
        if r is None or r is not UNKNOWN and r.broken:
            continue
        # (and then, while TestGrid's tabulator may not have written the tab again, the columns read
        # before by its filters: TABULATE more, for reads made by then)
        windows = [(lo, hi, -math.inf, made_by)] + \
            ([(hi, hi + TABULATE, -math.inf, min(hi, made_by))] if math.isfinite(hi) else [])
        if since is not None:  # (what it showed from then on: reads made before still count)
            windows = [(max(a, since), b, c, d) for a, b, c, d in windows if b >= since]
        for lo, hi, made_lo, made_hi in windows:
            if r is UNKNOWN:  # (by the options known for the tab: what TestGrid read then is not known)
                if shown_during(col_of(rule.get("options")), lo, hi):
                    verdicts.append(("undecided", []))
                continue
            if not any(shown_during(col_of(options), lo, hi) for options, _, _ in made_with):
                continue  # (a config the tab had only while it showed no read of the build)
            tab = dict(rule, shows=None if r.filter_problem else r.shows)
            for options, span, problem in made_with:
                col = col_of(options)
                span = (max(span[0], made_lo), min(span[1], made_hi))
                if span[0] > span[1]:
                    continue
                if problem:  # (reads made under options the port does not follow, if the tab showed one then)
                    if any(a <= hi and lo <= b and a <= span[1] and span[0] <= end
                           for _, windows_ in getattr(col, "reads", [(col, [(-math.inf, math.inf, math.inf)])])
                           for a, b, end in windows_):
                        verdicts.append(("undecided", []))
                    continue
                verdicts.append(config_verdict(col, job, dict(tab, problem=None), made, (lo, hi), span))
    return combine(verdicts) if verdicts else ("not-red", [])


def config_verdict(col, job, rule, made, during=None, read_in=None):
    """verdict() on a tab by one config of it (in force `during`, see verdict): undecided
    for a tab whose columns the port cannot paint as TestGrid does ("problem"); by its
    config's filters ("shows"), or, where they cannot be applied, the rows its table showed
    ("rows"); and both, red when either is, for a build made before the tab's config last
    agreed with its table ("unreliable_until")."""
    if rule.get("problem"):
        return "undecided", []
    shows = rule.get("shows")
    if shows is None:
        return verdict(col, job, rule.get("rows"), during=during, read_in=read_in)
    try:
        by_config = verdict(col, job, rule.get("rows"), shows, during, read_in)
    except tgc.Unsupported:  # (its filters ran out of time: TabRule keeps that as its filter_problem)
        return verdict(col, job, rule.get("rows"), during=during, read_in=read_in)
    if made < rule.get("unreliable_until", -math.inf):
        return combine([by_config, verdict(col, job, rule.get("rows"), during=during, read_in=read_in)])
    return by_config


def column_verdict(col, job, rows=None, shows=None):
    """Whether one column TestGrid read is red on a tab: ("red", its red rows there) or
    ("not-red", []) (see verdict())."""
    if shows is None:
        rows = rows or {}
        hidden = {job + ".Overall"} if not rows.get("overall", True) else set()
        hidden |= {job + ".Pod"} if not rows.get("pod", True) else set()
        shows = lambda row: row not in hidden  # noqa: E731
    if col.errors:
        # (a column TestGrid could not read has one row, "Overall"; a tab that hides it shows no such column)
        if "TOOL_FAIL" in col.results and shows("Overall"):
            return "red", [{"test": "Overall", "status": "TOOL_FAIL", "message": "TestGrid could not read the build from GCS"}]
        return "not-red", []  # still running, too old, or finished without ever starting: not red
    red = [row for row in col.red_rows() if shows(row)]
    return ("red", [{"test": row, "status": col.cells[row], "message": col.notes.get(row) or None} for row in red]) \
        if red else ("not-red", [])


def tab_rows(table, job, results=False):
    """Whether a tab shows its job's Overall and Pod rows. Every column TestGrid reads
    has both, so a tab's base options hide the one missing from its table while the
    other is there (gce-master-scale-correctness hides Overall), and both when its
    columns hold `results` without either; a table without results says nothing,
    and both count as shown."""
    names = {t.get("name") for t in table.get("tests", []) if isinstance(t, dict)}
    overall, pod = f"{job}.Overall" in names, f"{job}.Pod" in names
    if not overall and not pod and results:
        return {"overall": False, "pod": False}
    return {"overall": overall or not pod, "pod": pod or not overall}


def known_rows(rows):
    """A tab's rows as state.json remembers them, None if that is not what it holds."""
    if isinstance(rows, dict) and set(rows) == {"overall", "pod"} and all(isinstance(v, bool) for v in rows.values()):
        return rows
    return None


UNKNOWN = "unknown"  # (a tab's config when no run of the archive has read it yet)


entry_id = tgc.entry_id


class ConfigHistory:
    """TestGrid's config over time, as the archive learned it (see testgrid_config):
    TestGrid production takes the config each run of test-infra's upload job uploads,
    within PICKUP of the run's end. .runs: those runs, oldest first, each {"build",
    "commit", "started", "finished" (None while it runs), "tabs": {"dashboard#tab": entry}
    (None while its config is not known), "group_changes": how it changed its tabs' test
    groups ({name: "new" or "changed"}; None while not known, see events), "listed": when
    the archive first listed it (None: before it kept that), "failed": why its config could
    not be built the last time that was tried (None: it was not, or it is known), "recipe":
    what names its config (testgrid_config.recipe_id; None: not kept), "built": what its config
    was last built from ({"committed": test-infra's commit time, "image", "digest"}; None: not
    known)}, back to the last that began CONFIG_KEEP_DAYS ago. In state.json (encode()): "config_uploads",
    the runs, each with the tabs whose entry it changed (a tab gone: null), every tab's for
    a "base" one (the first, and the first after one not known), and the digest of its
    whole config of the tabs ("config_digest", checked when read back); "config_entries",
    {id: entry}; and "config_relist_after" while a run dropped from state.json (malformed)
    is to be listed again (testgrid_config.plan lists the runs made after it)."""

    def __init__(self, uploads=None, entries=None, warnings=None, relist_after=None, now=None):
        self.runs, relist = tgc.decode_uploads(uploads, entries, time.time() if now is None else now)
        self.recipes = {}  # per run listed now, what names its config (testgrid_config.recipe_id)
        self.relist_after = relist_after if tgc.is_moment(relist_after) else None
        if relist is not None:
            self.relist_after = relist if self.relist_after is None else min(relist, self.relist_after)
            if warnings is not None:
                warnings.append("state.json: dropped malformed runs of TestGrid's config uploads; the runs of the "
                                f"upload job from {iso(relist) if relist else 'the first one kept'} on are listed "
                                "again")

    def merge(self, loaded, now=None):
        """Take what testgrid_config.sh read now (testgrid_config.load): the upload job's runs
        made after loaded["after"] (one that still ran before and is not listed now failed:
        it uploaded nothing), the configs built, those that could not be, and how runs
        changed their test groups. now: when they were listed (a run first listed then)."""
        after, configs, changes = loaded["after"], loaded["configs"], loaded["group_changes"]
        failed = loaded.get("failed") or {}
        first = min((created(r["build"]) for r in self.runs), default=None)  # (of the runs kept before)
        known = {r["build"]: r for r in self.runs}
        listed = {r["build"] for r in loaded["runs"]}
        for b, r in list(known.items()):
            if r["finished"] is None and tgc.made(b) > after and b not in listed:  # (as plan() lists: by the millisecond)
                del known[b]
        for r in loaded["runs"]:
            self.recipes[r["build"]] = r["recipe"]
            had = known.get(r["build"])
            if had is None:
                known[r["build"]] = dict({k: r[k] for k in ("build", "commit", "started", "finished", "recipe")},
                                         tabs=None, group_changes=None, listed=now, failed=None, built=None)
            else:
                had["recipe"] = r["recipe"]
                if had["finished"] is None:
                    had["finished"] = r["finished"]
        for r in known.values():
            recipe = self.recipes.get(r["build"])
            built = {k: v for k, v in configs[recipe][1].items() if k != "test_infra"} if recipe in configs else None
            if tgc.valid_built(built):  # (what its config was built from: test-infra's commit time, the image)
                r["built"] = built
            if r["tabs"] is None and recipe in configs:
                r["tabs"] = configs[recipe][0]
            if r["group_changes"] is None and r["tabs"] is not None and r["build"] in changes:
                r["group_changes"] = changes[r["build"]]
            if r["tabs"] is not None:
                r["failed"] = None
            elif recipe in failed:
                r["failed"] = str(failed[recipe])[:300]
        self.runs = sorted(known.values(), key=lambda r: int(r["build"]))
        # (listed again from where it was asked; for 0, from the first run kept, or for good with none kept)
        if self.relist_after is not None and (after <= self.relist_after or
                                              self.relist_after == 0 and (first is None or after < first)):
            self.relist_after = None

    def prune(self, now):
        """Forget the runs before the last that began CONFIG_KEEP_DAYS ago."""
        old = [i for i, r in enumerate(self.runs) if r["started"] <= now - CONFIG_KEEP_DAYS * 86400]
        if old:
            self.runs = self.runs[old[-1]:]

    def encode(self):
        """(state.json's "config_uploads", its "config_entries")."""
        uploads, entries, prev = [], {}, None
        for r in self.runs:
            u = {k: r[k] for k in ("build", "commit", "started", "finished")}
            u.update((k, r[k]) for k in ("listed", "failed", "recipe", "built") if r.get(k) is not None)
            if r["tabs"] is None:
                u["tabs"], prev = None, None
            else:
                ids = {k: entry_id(e) for k, e in r["tabs"].items()}
                entries.update((ids[k], e) for k, e in r["tabs"].items())
                if prev is None:
                    u["tabs"], u["base"] = ids, True
                else:
                    u["tabs"] = {k: ids.get(k) for k in sorted(set(ids) | set(prev)) if ids.get(k) != prev.get(k)}
                u["config_digest"] = tgc.tabs_digest(ids)
                prev = ids
                if r.get("group_changes") is not None:
                    u["group_changes"] = r["group_changes"]
            uploads.append(u)
        return uploads, entries

    def current(self):
        """(the tabs' entries, the run) of the latest run that ended whose config is known; (None, None)."""
        return next(((r["tabs"], r) for r in reversed(self.runs) if r["tabs"] is not None and r["finished"]),
                    (None, None))

    def unknown(self):
        """The runs whose config is not known yet."""
        return [r for r in self.runs if r["tabs"] is None]

    def ended(self, i):
        """When the run at i ended, at the latest: its finish; for one without (still running,
        or its finished.json never written), when the next run began, as the upload job runs
        one at a time (unless its runs show otherwise: then inf), else inf."""
        r = self.runs[i]
        if r["finished"] is not None:
            return r["finished"]
        alone = all(a["finished"] is None or a["finished"] <= b["started"] for a, b in itertools.pairwise(self.runs))
        return self.runs[i + 1]["started"] if i + 1 < len(self.runs) and alone else math.inf

    def taken(self, key, at, slack=0):
        """Whether a config with the tab may be TestGrid's at `at` (or until `slack` after it was
        replaced): one whose run had uploaded it by then (ended), not replaced before then."""
        return any(r["tabs"] is not None and r["tabs"].get(key) is not None
                   and self.ended(i) < at <= self.replaced(self.ended(i)) + slack for i, r in enumerate(self.runs))

    def replaced(self, after):
        """When the config in force at `after` was replaced at the latest: by the upload of a
        run that began then or later, by its finish (and PICKUP)."""
        return min((r["finished"] + PICKUP for r in self.runs if r["finished"] is not None and r["started"] >= after),
                   default=math.inf)

    def eras(self, key, lacked=None):
        """The tab's entry over time, as [(from, to, entry)]: entry None while TestGrid had no
        such tab, UNKNOWN while the archive does not know it; a run's entry from its start
        until a run that began after it ended uploaded its config (and PICKUP: either may be
        in force then; and the tabulator may show the columns read before by its filters
        TABULATE longer, see tab_verdict). Runs with the same entry make one era. A tab TestGrid
        listed while the latest config known lacked it (`lacked`: when it last did, see run()):
        the runs without it that began before then, after the last run with it that had uploaded
        its config by then (ended), have it as not known, each in an era of its own (their
        configs of it may differ)."""
        if not self.runs:
            return []
        known = [UNKNOWN if r["tabs"] is None else r["tabs"].get(key) for r in self.runs]
        entries = known
        if lacked is not None:
            last = max((i for i, e in enumerate(known) if e is not None and e is not UNKNOWN and self.ended(i) < lacked),
                       default=-1)
            entries = [UNKNOWN if e is None and i > last and r["started"] < lacked else e
                       for i, (r, e) in enumerate(zip(self.runs, known, strict=True))]
        # (one taken as not known so stays apart from the runs next to it, one not known yet or taken
        # so too: config_changed sees a change from its config, or to it, between two eras, as events
        # has none for it)
        out, was = [(-math.inf, self.replaced(-math.inf), UNKNOWN)], [UNKNOWN]
        for i, (r, entry, before) in enumerate(zip(self.runs, entries, known, strict=True)):
            end = self.replaced(self.ended(i))
            if out[-1][2] == entry and was[-1] == before and not (entry is UNKNOWN and before is None):
                out[-1] = (out[-1][0], max(out[-1][1], end), entry)  # (one of them may end later than the last)
            else:
                out.append((r["started"], end, entry))
                was.append(before)
        return out

    @staticmethod
    def group_change(prev, r, group, entry):
        """How run r changed the test group named `group` (its entry in r's config: `entry`)
        from the run before (prev): "new", "changed" or None, or "unknown" where the archive
        cannot tell yet: by the group's digest in the run before's tabs where the run's own
        comparison was not made. A changed group whose days of results were REPROCESS_DAYS or fewer
        counts as new: TestGrid reads again every column newer than that (reprocessColumn),
        and lists every build after the highest of the columns it keeps (hintStarted): such a
        grid keeps none older, so it reads every build, as a new group's first update does."""
        if r.get("group_changes") is not None:
            how = r["group_changes"].get(group)
        elif prev["tabs"] is None:
            return "unknown"
        else:  # (by the run before's tabs: a digest not known tells nothing)
            digests = {e.get("group_digest") for e in prev["tabs"].values()
                       if e.get("test_group") == group and e.get("group") is not None}
            how = "unknown" if not digests or None in digests or entry.get("group_digest") is None else \
                None if digests == {entry["group_digest"]} else "changed"
        if how == "changed":  # (its days of results before not known: taken as "new")
            before = [e["group"] for e in (prev["tabs"] or {}).values()
                      if e.get("test_group") == group and e.get("group") is not None]
            if not before or tgc.group_days(before[0]) <= REPROCESS_DAYS:
                how = "new"
        return how

    def events(self, key, now, prefix=None):
        """When an update may begin that reads again the columns of a test group the tab has
        shown (one that reads builds from the GCS prefix `prefix`, if given), as a run of the
        upload job changed the group: [(from, to, how)], how "changed" (any field of the
        TestGroup: TestGrid reads again the columns of the builds that started in the 7 days
        before the update, reprocessColumn), "new" (a group TestGrid did not have: its first
        update reads every build it shows; see group_change), or "unknown": a run whose config,
        or whose change to the group, is not known yet, taken as "new" (until it is known). A
        group's changes count whatever tab showed it then: TestGrid's updater reads by test
        group."""
        def reads(e):
            return e is not None and e.get("group") is not None and \
                (prefix is None or tgc.group_source(e["group"])[0] == prefix)
        groups = sorted({e["test_group"] for r in self.runs if r["tabs"] for e in [r["tabs"].get(key)] if reads(e)})
        out = []
        for i in range(1, len(self.runs)):
            prev, r = self.runs[i - 1], self.runs[i]
            if r["tabs"] is None:
                how = "unknown"
            else:
                hows = set()
                for g in groups:
                    e = next((x for x in r["tabs"].values() if x.get("test_group") == g and reads(x)), None)
                    if e is not None:  # (no tab of the run has the group: nothing is known of it then)
                        hows.add(self.group_change(prev, r, g, e))
                how = "new" if "new" in hows else "+".join(h for h in ("changed", "unknown") if h in hows) or None
            if how:
                end = self.ended(i)
                end = now if end == math.inf else min(end, now)
                # (a known change and an unknown one of another group: both, the unknown taken as new)
                out += [(r["started"], end + PICKUP + READ_GAP - UPDATE_SPAN, h) for h in how.split("+")]
        return out


def config_changed(rule, since, newest=math.inf):
    """Whether a tab's config (its rule's "eras" and "events", see history_rules) may have
    changed what it shows of builds made up to `newest` after time `since`: a config of it
    in force until after then, and then one with other filters (its other base options
    only draw the tab), or none, or one whose test group reads another job; or an update
    after a change to its test group that may begin then and read such a build again.
    True, False, or None while that is not known (a config the archive does not know yet
    was in force then)."""
    def shows(r):
        return None if r is None else (r.prefix, r.filters) if isinstance(r, tgc.TabRule) else r
    unknown = False
    for (_, hi, a), (_, _, b) in itertools.pairwise(rule.get("eras") or ()):
        if hi + TABULATE > since:  # (the tabulator may show the old filters until then)
            if a is UNKNOWN or b is UNKNOWN:
                unknown = True
            elif shows(a) != shows(b):
                return True
    for lo, hi, how in rule.get("events") or ():
        if hi > since and (how != "changed" or lo < newest + PENDING + REPROCESS_DAYS * 86400):
            if how != "unknown":
                return True
            unknown = True
    return None if unknown else False


def pending_dropped(key, p, why):
    """The serious warning for a range of builds a scan saw (a tab's "changed_pending", see run())
    that the archive stops waiting to judge."""
    return (f"{key}: the builds made from {iso(p['lo'])} to {iso(p['hi'])}, seen not red by the scan of "
            f"{iso(p['since'])}, cannot be judged from GCS: {why}; no longer judged, though TestGrid may have "
            "shown them red")


def lacking_kept(lacked, now):
    """The tabs of state.json's lacking_tabs (see run()) whose record still counts at now: for as
    long as a build held for the tab may wait, and a day more. cross_check.py reads them so too."""
    return {k: t for k, t in lacked.items() if now - t <= (HELD_DAYS + 1) * 86400}


def first_listed(run):
    """When the archive first listed a run of the upload job (for one listed before it kept
    that: when the run ended, or began)."""
    return run.get("listed") if run.get("listed") is not None else run["finished"] or run["started"]


def judged_since(state, now):
    """From when a build the archive may still judge from GCS (as of state.json) could have been
    shown, so the config TestGrid had then may decide it: one in a gap a run finds now (searched
    GIVE_UP_DAYS back), one watched, one in a part of a gap not searched yet, and one a scan saw
    before its tab's config changed. (A build the archive saw on its tab is judged by what the
    tab showed, whatever config it had.)"""
    times = [now - GIVE_UP_DAYS * 86400]
    for t in state["tabs"].values():  # (as load_state checked them)
        times += [t["unsearched"][0]] if "unsearched" in t else []
        times += [p["since"] for p in t.get("changed_pending", ())]
    times += [u["rec"]["created"] for u in state["watch"].values()]
    return min(times)


def history_rules(history, keys, now, lacking=None, queries=None, era_rule=None):
    """Per tab ("dashboard#tab", of `keys`), what its config over time (a ConfigHistory) says
    for tab_verdict: {"eras": its configs (see ConfigHistory.eras; era_rule(entry) makes each
    entry's rule), "events": the updates that read its columns again (see
    ConfigHistory.events: of the test groups it showed that read its job, queries[key])}.
    A tab TestGrid showed while the latest config known lacked it (`lacking`: {key: when it
    last did}): its configs then are not known, as ConfigHistory.eras takes them. Tabs without
    eras are left out."""
    lacking, queries, era_rule = lacking or {}, queries or {}, era_rule or (lambda entry: entry)
    out = {}
    for key in keys:
        eras = [(lo, hi, era_rule(entry)) for lo, hi, entry in history.eras(key, lacking.get(key))]
        if eras:
            out[key] = {"eras": eras, "events": history.events(key, now, queries.get(key))}
    return out


def config_about(source):
    """Where a config came from, for messages (its source as testgrid_config.load and run() give it)."""
    text = f"test-infra {str(source.get('test_infra'))[:12]}"
    if plausible_time(source.get("committed"), time.time()):
        text += f" of {iso(source['committed'])}"
    if plausible_time(source.get("uploaded"), time.time()):
        text += f", uploaded {iso(source['uploaded'])}"
    return text


def read_config(path, warnings):
    """The upload job's runs, the configs built and the rest testgrid_config.sh wrote to
    `path` (testgrid_config.load), or None with a warning."""
    try:
        return tgc.load(path)
    except (OSError, ValueError) as e:
        warnings.append(f"TestGrid's config could not be read from {path} ({type(e).__name__}: {str(e)[:200]})")
        return None


def config_mismatch(rule, table, query):
    """How a tab's table disagrees with its config (a testgrid_config.TabRule): builds
    read from another GCS prefix, or rows shown that its base options hide; "" when it
    agrees, None when that is not known (its filters cannot be applied, or TestGrid does
    not update its test group: what the table shows is none of the config's doing)."""
    if rule.not_updated:
        return None
    problems = []
    if rule.prefix != query:
        problems.append(f"its test group reads gs://{rule.prefix}, the tab shows gs://{query}")
    try:
        hidden = [t["name"] for t in table.get("tests", []) if isinstance(t, dict) and isinstance(t.get("name"), str)
                  and not rule.shows(t["name"])]
    except (tgc.Unsupported, tgc.Broken):  # (TabRule.filter_problem or .broken says why)
        return "; ".join(problems) or None
    if hidden:
        problems.append(f"the tab shows {len(hidden)} rows its base options hide, such as {hidden[0][:200]!r}")
    return "; ".join(problems)


def tab_days(oldest, scanned):
    """The tab's days of results as far as its history shows (TestGrid's default when shorter)."""
    if not isinstance(oldest, (int, float)) or isinstance(oldest, bool) or not isinstance(scanned, (int, float)):
        return tr.DAYS_OF_RESULTS
    return max(tr.DAYS_OF_RESULTS, math.ceil((scanned - oldest) / 86400))


def unchanged_in_gcs(url, info):
    """Whether GCS still holds the content an archived file was taken from.

    An object GCS no longer has counts as unchanged: the archived copy is the
    only one left, so there is nothing newer to fetch. A record written before
    GCS identities were kept gets the identity it was just found to match.
    """
    try:
        headers = request(url, consume=lambda r: r.headers, method="HEAD", headers={"Accept-Encoding": "gzip"})
    except NotFound:
        return True
    now = remote_identity(headers)
    if not same_object(recorded_identity(info), now):
        return False
    info.setdefault("gcs", now)
    return True


def in_gcs(url):
    """Whether GCS has an object (asked with a HEAD)."""
    try:
        request(url, consume=lambda r: r.headers, method="HEAD", headers={"Accept-Encoding": "gzip"})
    except NotFound:
        return False
    return True


def left_as_it_is(name, base, error=None):
    """The error of a build whose folder's meta.json cannot be read as the build's (see store), and whose
    copy of name it cannot take from GCS again."""
    why = "GCS no longer has" if error is None or isinstance(error, NotFound) else \
        f"GCS cannot serve ({type(error).__name__}: {error})"
    return RuntimeError(f"its meta.json cannot be read as this build's, and {why} {name}, of which its folder holds "
                        f"a copy: the copy and the meta.json are left as they are ({base})")


def holds_copy(target, name):
    """Whether a build folder holds a stored copy of name (build-log.txt or podinfo.json): as it came,
    gzipped, or in parts."""
    for n in listdir(target):
        m = PART_RE.fullmatch(n)
        if (m.group(1) if m else n) in (name, name + ".gz"):
            return True
    return False


def settle_flags(meta, checked, now):
    """(settled, final) for an archived copy found equal to GCS at time `checked`.

    settled: the copy was taken or confirmed SETTLE_HOURS after the build
    finished, when Prow had stopped uploading. final: it was confirmed
    RECHECK_HOURS after that, so later runs compare it only in its seventh of
    the archive (see gcs_sevenths). A build without finished.json, or without a
    log, keeps being checked for a late one on every run until GIVE_UP_DAYS after
    it was first archived.
    """
    fin = meta.get("finished")
    if not plausible_time(fin, now) or fin < meta["created"] - 86400:
        fin = None  # not a time this build could have finished at
    first = meta.get("archived_at") if plausible_time(meta.get("archived_at"), now) else meta["created"]
    settled = fin is not None and checked >= fin + SETTLE_HOURS * 3600
    if fin is None or not meta.get("log"):
        return settled, checked - first >= GIVE_UP_DAYS * 86400
    return settled, checked >= fin + RECHECK_HOURS * 3600


# A comparison with GCS that fails (GCS refuses a file, or serves one that cannot be verified) is an error of the
# run, and the next run compares the build again. meta.json keeps when the first run that failed began, until one
# succeeds. A run that fails once GIVE_UP_DAYS have passed since gives up, with an error: the build is final, and
# meta.json keeps that run's failure (COMPARISON_GIVEN_UP). Later runs still compare it in its seventh (see
# gcs_sevenths), and one that succeeds removes both, but a failure on GCS's side there is no error (see
# NotCompared). A comparison the disk fails, or a bug in this code (BUGS), is an error that neither starts
# nor ends a build's failures.
COMPARISON_FAILING = "comparison_failing_since"
COMPARISON_GIVEN_UP = "comparison_given_up"
OLD_GAVE_UP_NOTE = "not compared with GCS after "  # (the note code before COMPARISON_FAILING wrote for it)


def given_up_on(meta):
    """Whether the runs gave up on this archived build after its comparisons with GCS failed (see
    COMPARISON_FAILING)."""
    note = meta.get("note")
    return isinstance(meta.get(COMPARISON_GIVEN_UP), str) or isinstance(note, str) and note.startswith(OLD_GAVE_UP_NOTE)


def failing_since(meta, now):
    """When the first run that failed to compare this build with GCS began (COMPARISON_FAILING); None if none
    failed since one succeeded, or if meta.json holds no time they could have failed since (more than a day
    before the build was made, when it says when that was)."""
    since, made = meta.get(COMPARISON_FAILING), meta.get("created")
    if not plausible_time(since, now) or plausible_time(made, now) and since < made - 86400:
        return None
    return since


def recheck(old, rec, target, gcs, run_id, now, compress, max_bytes, compare_final=False):
    """An intact archived build: add new tab sightings and, until it is final (or when
    compare_final: a final build in this run's sevenths, see gcs_sevenths), compare its
    files with GCS and fetch again whatever GCS now holds instead. A comparison that
    fails raises RecheckError, until the run that gives up on the build returns
    "comparison-given-up"; one that fails on GCS's side after that raises
    NotCompared (see COMPARISON_FAILING)."""
    tabs, _ = merge_tabs(old["tabs"], rec["tabs"])
    meta = copy.deepcopy(dict(old, tabs=tabs))  # the file records may gain a GCS identity below
    if meta.get("started") is None and rec.get("started") is not None:
        meta["started"] = rec["started"]  # taken from GCS in a gap, then shown by TestGrid
    if meta != old:  # new sightings are recorded first: a GCS check that fails below must not lose them
        write_json(os.path.join(target, "meta.json"), meta)
        old = copy.deepcopy(meta)
    outcome = "already-archived"
    if not old.get("final") or compare_final:
        try:
            # (where its files are: as its record says, for a pull request job's, or see build_folder)
            try:
                where = stored_folder(old) or build_folder(gcs, rec["query"], rec["build"])
            except NoSuchBuild:  # (GCS no longer has it: compared as one GCS no longer has under its query)
                where = f"{rec['query']}/{rec['build']}"
            base = f"{gcs}/{where}"
            if where != f"{rec['query']}/{rec['build']}" and meta.get("gcs_path") != where:
                # (a pull request job's build archived by code that read it under Prow's index)
                meta.update(gcs_path=where, log_url=f"{base}/build-log.txt",
                            prow_url=f"https://prow.k8s.io/view/gs/{where}")
                if str(meta.get("note", "")).startswith("GCS had neither finished.json nor build-log.txt"):
                    del meta["note"]  # (said of the folder under the index, not of its own)
                put_in_place(target, meta)  # (kept, as the sightings are; a disk error is a LocalIOError)
            finished = read_finished(base)
            changed = []
            # (a file taken again goes in place with the meta.json that says so, see put_in_place; meta stays
            # what is in place, for a failure below to keep)
            if meta.get("log"):
                if not unchanged_in_gcs(base + "/build-log.txt", meta["log"]):
                    log = take_copy(base, target, compress, max_bytes)
                    new = dict(meta, log=log.info)
                    if log.info["md5"] != meta["log"].get("md5"):  # an old record may only lack GCS's identity
                        changed.append("build-log.txt")
                        new.update(refreshed_by_run=run_id, refreshed=list(changed))
                    put_in_place(target, new, [log])
                    meta = new
            else:
                try:  # archived without a log: has one appeared since?
                    log = take_copy(base, target, compress, max_bytes)
                    new = dict(meta, log=log.info, log_recovered_by_run=run_id)
                    put_in_place(target, new, [log])
                    meta, outcome = new, "log-recovered"
                except NotFound:
                    pass
            # (one that cannot be read fails the comparison, as the log does: the next run compares it again)
            if meta.get("podinfo") and not unchanged_in_gcs(base + "/podinfo.json", meta["podinfo"]):
                pod = take_copy(base, target, compress, max_bytes, "podinfo.json")
                new = dict(meta, podinfo=pod.info)
                if pod.info["md5"] != meta["podinfo"].get("md5"):
                    changed.append("podinfo.json")
                    new.update(refreshed_by_run=run_id, refreshed=list(changed))
                put_in_place(target, new, [pod])
                meta = new
            elif not meta.get("podinfo"):
                try:  # (Prow writes it up to minutes after finished.json; code before kept it only without a log)
                    pod = take_copy(base, target, compress, max_bytes, "podinfo.json")
                    changed.append("podinfo.json")
                    new = dict(meta, podinfo=pod.info, refreshed_by_run=run_id, refreshed=list(changed))
                    put_in_place(target, new, [pod])
                    meta = new
                except NotFound:
                    pass
        except Abandoned:
            raise
        except (LocalIOError, *BUGS) as e:  # (not GCS's doing: an error that neither starts nor ends its failures)
            raise RecheckError(f"comparing the archived copy with GCS failed: {type(e).__name__}: {e}", meta) from None
        except Exception as e:  # (GCS's doing: see COMPARISON_FAILING)
            error = f"{type(e).__name__}: {e}"
            folder = str(meta.get("log_url") or "").rsplit("/", 1)[0]
            short = error.replace(folder + "/", "") if folder else error  # (the build's own URL: what fails is after it)
            if given_up_on(meta):
                raise NotCompared(short, meta) from None
            since = failing_since(meta, now)
            if since is not None and now - since <= GIVE_UP_DAYS * 86400:
                raise RecheckError(f"comparing the archived copy with GCS failed: {error}", meta) from None
            if since is None:  # (the first run to fail since one succeeded)
                meta[COMPARISON_FAILING] = now
            else:
                meta.update(final=True, **{COMPARISON_GIVEN_UP: short[:300]})
            try:  # (meta is what is in place: a file taken before the failure is in it)
                put_in_place(target, meta)
            except (LocalIOError, *BUGS) as disk:
                raise RecheckError(f"comparing the archived copy with GCS failed: {error} (and its meta.json could "
                                   f"not record that: {type(disk).__name__}: {disk})", meta) from None
            if since is None:
                raise RecheckError(f"comparing the archived copy with GCS failed: {error}", meta) from None
            return "comparison-given-up", meta
        if changed:
            meta.update(refreshed_by_run=run_id, refreshed=changed)
            if outcome == "already-archived":
                outcome = "refreshed"
        if isinstance(meta.get("note"), str) and meta["note"].startswith(OLD_GAVE_UP_NOTE):
            del meta["note"]  # (compared now, after all)
        meta.pop(COMPARISON_FAILING, None)
        meta.pop(COMPARISON_GIVEN_UP, None)
        if finished is not None:  # a field this read could not tell keeps what was recorded
            if finished["result"] is not None:
                meta["result"] = finished["result"]
            if finished["timestamp"] is not None:
                meta["finished"] = finished["timestamp"]
        meta["settled"], meta["final"] = settle_flags(meta, time.time(), now)
    if meta != old:
        try:
            put_in_place(target, meta)
        except (LocalIOError, *BUGS) as e:  # (as in the comparison: what it found is not recorded, so compared again)
            raise RecheckError(f"comparing the archived copy with GCS failed: {type(e).__name__}: {e}", meta) from None
    return outcome, meta


def archive(rec, target, gcs, run_id, now, max_job_hours, compress=False, max_bytes=95 << 20, tab_rules=None,
            timelines=None, compare_final=False):
    """Archive one red build into `target` (its existing folder, or one in this run's folder).
    tab_rules: per tab ("dashboard#tab"), the rows it shows and its days of results, for a
    build from a history gap that is judged here (see judge_in_tabs, and its timelines).
    compare_final: compare it with GCS even if it is final (see recheck)."""
    meta_path = os.path.join(target, "meta.json")
    existed = os.path.exists(meta_path)
    try:
        old = read_json(meta_path)
    except ValueError:
        old = None  # corrupt meta.json: archive the build again
    old = old if valid_meta(old) and old.get("job") == rec["job"] and old.get("build") == rec["build"] else None
    if old and intact(target, old, deep=True):
        return recheck(old, rec, target, gcs, run_id, now, compress, max_bytes, compare_final)
    # (where its files are: as its record says, for a pull request job's, or see build_folder)
    where = (old and stored_folder(old)) or build_folder(gcs, rec["query"], rec["build"])
    base = f"{gcs}/{where}"
    if not rec.get("watch"):
        return store(rec, target, existed, old, read_finished(base), base, run_id, now, max_job_hours, compress,
                     max_bytes, where)
    # from a history gap, not decided when last looked at
    state, tabs, partial = judge_in_tabs(gcs, rec, tab_rules or {}, timelines)
    if state != "red":
        if state == "not-red":
            return "not-red", rec  # finished (or never started, or too old) and not red on any of its tabs
        if partial:  # finished, and only its junit files keep it from being judged: an error
            raise IntegrityError(f"cannot be judged by TestGrid's rule: {partial}")
        if held_by(rec, tab_rules or {}):  # until the port follows its tab's config, for as long as GCS keeps it
            return ("held" if now - rec["created"] <= HELD_DAYS * 86400 else "stopped-holding"), rec
        return ("watching" if now - rec["created"] <= GIVE_UP_DAYS * 86400 else "stopped-watching"), rec
    rec = {k: v for k, v in rec.items() if k != "watch"}
    rec["tabs"] = tabs
    rec["backfilled"] = backfill_note(tabs, partial)
    try:
        return store(rec, target, existed, old, read_finished(base), base, run_id, now, max_job_hours, compress,
                     max_bytes, where)
    except Exception as e:
        e.red_rec = rec  # now known to be red: retried and given up on like any other red build
        raise


def judged_with(rec, tab_rules):
    """(days of results, events) a watched build is judged with on its tabs (see judge_in_tabs)."""
    keys = [f"{t['dashboard']}#{t['tab']}" for t in rec["tabs"]]
    return (max([build_days(tab_rules.get(k, {}), rec["created"]) for k in keys] or [tr.DAYS_OF_RESULTS]),
            tab_events(tab_rules, keys))


def judge_in_tabs(gcs, rec, tab_rules, timelines=None):
    """("red", its tabs with their red cells) when TestGrid's column for the build is
    red on one of the tabs in rec["tabs"], else ("not-red" | "undecided", []); and the
    column's .partial (why its junit files were not read, else None). A tab whose
    config is not followed here (its rule's "problem") leaves the build undecided.
    timelines: the run's Timelines (else the build's job is read for it alone)."""
    rules = [tab_rules.get(f"{t['dashboard']}#{t['tab']}", {}) for t in rec["tabs"]]
    days, events = judged_with(rec, tab_rules)
    cols = Columns(gcs, rec["query"], rec["build"], days, timelines=timelines or Timelines(time.time()), events=events)
    col = cols()  # (read the build: a read that fails raises here)
    states, red = [], []
    for t, rule in zip(rec["tabs"], rules, strict=True):
        # (a tab whose scan saw the build before its config changed: what it showed since)
        since = t.get("since") if plausible_time(t.get("since"), time.time()) else None
        state, cells = tab_verdict(cols, rec["job"], rule, rec["created"], since=since)
        states.append(state)
        if state == "red":
            red.append(dict(t, red_cells=cells, judged_from_gcs=True))
    if red:
        return "red", red, getattr(col, "partial", None)
    return ("undecided" if "undecided" in states else "not-red"), [], unreadable_junit(col)


def held_by(rec, tab_rules):
    """Why the port cannot judge the build on one of its tabs (a test group option it
    does not follow, in a config the tab had while TestGrid may have shown the build; or
    such a config the archive does not know yet), or None."""
    for key in (f"{t['dashboard']}#{t['tab']}" for t in rec["tabs"]):
        rule = tab_rules.get(key) or {}
        eras = tab_eras(rule, rec["created"], rec.get("query"))
        # (for a time the archive does not know the config of: as the tab's config now says, and that
        # it is not known)
        problems = [rule.get("problem")] if eras is None else [
            r.group_problem if isinstance(r, tgc.TabRule) else
            rule.get("problem") or f"TestGrid's config of {key} then is not known yet" if r is UNKNOWN else None
            for _, _, r in eras]  # (Elsewhere: its group reads none of the build's job)
        found = next((p for p in problems if p), None)
        if found:
            return found
    return None


def backfill_note(tabs, partial=None):
    """Why a build judged from GCS was red, from its red cells (and, if its junit files could
    not be read, that its failing tests are not listed)."""
    reasons, tests = [], []
    for c in (c for t in tabs for c in t["red_cells"]):
        if c["test"] == "Overall":
            reason = "TestGrid could not read it from GCS (TOOL_FAIL)"
        elif c["test"].endswith(".Overall"):
            reason = c["message"] or f"{c['test']} {c['status']}"
        elif c["test"].endswith(".Pod"):
            reason = f"its pod: {c['message'] or c['status']}"
        else:
            tests.append(c["test"])
            continue
        if reason not in reasons:
            reasons.append(reason)
    if tests:
        reasons.append(f"failing tests: {', '.join(sorted(set(tests)))}")
    return (f"TestGrid did not show this build red to the archive (it scrolled off between runs, or still showed "
            f"it running); by TestGrid's rule it was red ({'; '.join(reasons)[:400]})"
            + (f"; {partial}, so its failing tests are not listed" if partial else ""))


def store(rec, target, existed, old, finished, base, run_id, now, max_job_hours, compress, max_bytes, where=None):
    """Take a red build's files from GCS, and put them in place in `target` with its meta.json (see
    put_in_place). where: the folder ("bucket/path") of its files in GCS, under base (see build_folder; else
    query/build)."""
    where = where or f"{rec['query']}/{rec['build']}"
    # A folder whose meta.json cannot be read as this build's (held: the copies it holds): no copy is known
    # to be intact, and a new meta.json lists only what this run takes from GCS (the end of the run removes
    # the rest). So a copy of a file it cannot take (GCS no longer has it, or cannot serve it) stays, and so
    # does the meta.json. GCS is asked first (a HEAD), not to download the others for nothing.
    held = {name for name in ("build-log.txt", "podinfo.json") if holds_copy(target, name)} if existed and not old \
        else set()
    for name in ("build-log.txt", "podinfo.json"):
        if name in held and not in_gcs(f"{base}/{name}"):
            raise left_as_it_is(name, base)
    os.makedirs(target, exist_ok=True)
    podinfo = note = None
    unread = False  # (its podinfo.json could not be read: the next run compares the build, and reads it again)
    # A repair takes again only the files it has no intact copy of: one intact stays, unasked of GCS,
    # which may have none now (it deletes old builds' files).
    kept = {key: (old or {}).get(key) for key in ("log", "podinfo")}
    whole = {key: bool(kept[key]) and intact(target, {key: kept[key]}, deep=True) for key in kept}
    copies = []  # (what it takes from GCS: put in place with the new meta.json, once all are taken)
    try:
        if whole["log"]:
            log = kept["log"]
        else:
            try:
                copies.append(take_copy(base, target, compress, max_bytes))
                log = copies[-1].info
            except NotFound:
                if kept["log"]:
                    raise RuntimeError(f"the archived log is damaged and GCS no longer has it ({base})") from None
                if "build-log.txt" in held:
                    raise left_as_it_is("build-log.txt", base) from None
                log = None
        if log is None:
            age = now - (rec["started"] or rec["created"])
            if finished is None and age <= 2 * max_job_hours * 3600:
                remove_build_dir(target)
                return "pending", rec  # still uploading: look again next run
            # The build is over without a log: its pod never ran (keep Prow's pod
            # status), or TestGrid gave up on it ("did not complete within 24
            # hours") and GCS has nothing. Either way keep the record; later runs
            # look for a late log for GIVE_UP_DAYS.
            if finished is None:
                note = f"GCS had neither finished.json nor build-log.txt {age / 3600:.0f}h after the build started"
        # Prow's record of the build's pod: its containers' ends (an OOMKilled, say) and its node. GCS may
        # not have it yet (Prow writes it up to minutes after finished.json): later runs look again (recheck).
        if whole["podinfo"]:
            podinfo = kept["podinfo"]
        else:
            try:
                copies.append(take_copy(base, target, compress, max_bytes, "podinfo.json"))
                podinfo = copies[-1].info
            except (Abandoned, LocalIOError):
                raise
            except Exception as e:
                if existed and isinstance(e, BUGS):
                    raise  # (a repair keeps the copy it has: a bug fails it, not taken for GCS refusing the file)
                if kept["podinfo"] and isinstance(e, NotFound):
                    raise RuntimeError(f"the archived podinfo.json is damaged and GCS no longer has it ({base})") from None
                if "podinfo.json" in held:
                    raise left_as_it_is("podinfo.json", base, e) from None
                if log is None and not isinstance(e, NotFound):
                    raise  # (the build's only file: archived once it can be read)
                unread = not isinstance(e, NotFound)
    except Exception:
        for c in copies:
            c.discard()
        remove_build_dir(target)
        raise
    tabs = merge_tabs(old["tabs"], rec["tabs"])[0] if old else rec["tabs"]
    home = os.path.basename(os.path.dirname(os.path.dirname(target)))  # the run folder that holds it
    meta = dict(rec,
                tabs=tabs,
                result=(finished or {}).get("result"),
                finished=(finished or {}).get("timestamp"),
                prow_url=f"https://prow.k8s.io/view/gs/{where}",
                log_url=base + "/build-log.txt",
                log=log,
                podinfo=podinfo,
                archived_by_run=home,
                archived_at=folder_time(home) or now)
    if where != f"{rec['query']}/{rec['build']}":
        meta["gcs_path"] = where  # (a pull request job's build: where Prow's index links it)
    if (old or {}).get("backfilled"):
        meta["backfilled"] = old["backfilled"]
    if note:
        meta["note"] = note
    if existed:
        meta.update(repaired_by_run=run_id, repaired=[c.name for c in copies])
    meta["settled"], meta["final"] = settle_flags(meta, time.time(), now)
    if unread:
        meta["final"] = False  # (the next run compares it, and so reads its podinfo.json again)
    # (a copy kept, not compared with GCS, or a file GCS did not serve now: what the runs know of its comparisons
    # failing stays, see COMPARISON_FAILING)
    if old and (any(whole.values()) or unread):
        meta.update({k: old[k] for k in (COMPARISON_FAILING, COMPARISON_GIVEN_UP) if k in old})
        if given_up_on(old):
            meta["final"] = True  # (as the run that gave up left it: compared in its seventh, which tries again a
            # podinfo.json left unread, once GCS serves the log)
            if not meta.get("note") and isinstance(old.get("note"), str) and old["note"].startswith(OLD_GAVE_UP_NOTE):
                meta["note"] = old["note"]
    put_in_place(target, meta, copies)
    return ("archived" if log else "archived-without-log") if not existed else "repaired", meta


def run_key(name):
    """Chronological sort key of a run folder name: its UTC start, then its suffix (none = 1)."""
    m = RUN_RE.fullmatch(name)
    return (name[:18], int(m.group(2) or 1)) if m else (name, 0)


def run_dirs(root):
    """Run folders as paths relative to root ("runs/<YYYY-MM>/<run>"), oldest first."""
    out = []
    runs = os.path.join(root, "runs")
    for month in sorted(listdir(runs)) if real_dir(runs) else []:
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}", month) or not real_dir(os.path.join(runs, month)):
            continue
        for name in listdir(os.path.join(runs, month)):
            m = RUN_RE.fullmatch(name)
            if m and m.group(1) == month and folder_time(name) is not None \
                    and real_dir(os.path.join(runs, month, name)):
                out.append(f"runs/{month}/{name}")
    return sorted(out, key=lambda rel: run_key(rel.rsplit("/", 1)[1]))


def read_report(root, rel):
    """The report (run.json) of the run folder rel: None if it is missing, cannot be read, or is a
    link (a link in the archive is never followed)."""
    path = os.path.join(root, rel, "run.json")
    try:
        return None if os.path.islink(path) else read_json(path)
    except (ValueError, OSError):
        return None


def stopped(run):
    """Whether a run report (see read_report) is the one a run has until it ends (with the error
    STOPPED): the run was cut off after it wrote it."""
    errors = run.get("errors") if isinstance(run, dict) else None
    return isinstance(errors, list) and any(isinstance(e, dict) and e.get("error") == STOPPED for e in errors)


def run_started(path):
    """A run report's start time, None if it is unreadable or not a time it could have started at
    (or a link: a link in the archive is never followed)."""
    try:
        started = None if os.path.islink(path) else read_json(path).get("started")
    except (OSError, ValueError, AttributeError):
        return None
    return started if plausible_time(started, time.time()) else None


def sevenths_done(root, rel):
    """Whether the run in folder rel was done with its sevenths of the archive (see gcs_sevenths):
    its fetch compared all it was due to with GCS ("gcs_compared" in its run.json; a build the runs
    gave up on, in that run or before, counts even if its comparison failed on GCS's side: see
    COMPARISON_FAILING), and its verification compared them too and found none differing ("differ"
    in its verification.json), whatever else it found (a tab that cannot be read, say)."""
    try:
        path = os.path.join(root, rel, "run.json")
        run = None if os.path.islink(path) else read_json(path)  # (a link in the archive is never followed)
        path = os.path.join(root, rel, "verification.json")
        v = None if os.path.islink(path) else read_json(path)
    except (OSError, ValueError):
        return False
    scope = v.get("scope") if isinstance(v, dict) else None
    return isinstance(run, dict) and run.get("gcs_compared") is True and isinstance(scope, dict) \
        and is_int(scope.get("differ")) and scope["differ"] == 0


def gcs_sevenths(root, t0):
    """The sevenths of the archived builds (see sampled()) that the run started at t0 compares
    with GCS again, final or not: one for each UTC day from the day after the last run before it
    that was done with its own (see sevenths_done) to the day of this one, and that run's day too
    if a run later that day was not; at least this day's, so all seven when no run of the week
    before was done with them. Each build is so compared, and verified, at least once a week, even
    when a day has no run, or a run is cut off or its verification finds a difference. The first run
    (none before it) compares this day's only. cross_check.py compares the same ones when it
    verifies that run."""
    day = int(t0 // 86400)
    earlier, after, first = False, None, day - 6  # (after: the day of the earliest run not done since)
    for rel in reversed(run_dirs(root)):
        # (a run whose report is missing, as one killed before it wrote one, cannot be read, or has no
        # start a run can have: by when its folder was made, not done)
        started = run_started(os.path.join(root, rel, "run.json"))
        done = started is not None and sevenths_done(root, rel)
        started = started or folder_time(rel.rsplit("/", 1)[1])
        if started is None or started >= t0:
            continue
        earlier = True
        if started < (day - 7) * 86400:
            break  # (an older one cannot add a day)
        if done:
            first = int(started // 86400) + 1 if after is None else min(after, int(started // 86400) + 1)
            break
        after = int(started // 86400)
    if not earlier:
        first = day
    first = max(min(first, day), day - 6)
    return {d % 7 for d in range(first, day + 1)}


def sampled(key, sevenths):
    """Whether the build `key` ("job/build") is in one of these sevenths of the archive."""
    return zlib.crc32(key.encode()) % 7 in sevenths


def folder_time(name):
    """The UTC start time a run folder name encodes, None if it is not a real time."""
    try:
        return dt.datetime.strptime(name[:18], "%Y-%m-%dT%H%M%SZ").replace(tzinfo=dt.UTC).timestamp()
    except ValueError:
        return None


def archived_builds(root):
    """Where each archived build lives: ({"job/build": "runs/<month>/<run>/<job>/<build>"}, duplicates).

    A build counts once it has a meta.json; if two run folders hold the same
    build, the earliest wins and the others are reported.
    """
    found, duplicates = {}, []
    for run_rel in run_dirs(root):
        run_abs = os.path.join(root, run_rel)
        for job in sorted(listdir(run_abs)):
            if job in RUN_FILES or not JOB_RE.fullmatch(job) or not real_dir(os.path.join(run_abs, job)):
                continue
            for build in sorted(listdir(os.path.join(run_abs, job))):
                rel = f"{run_rel}/{job}/{build}"
                if BUILD_RE.fullmatch(build) and real_dir(os.path.join(root, rel)) \
                        and os.path.isfile(os.path.join(root, rel, "meta.json")):
                    key = f"{job}/{build}"
                    if key in found:
                        duplicates.append(f"{key} is in both {found[key]} and {rel}")
                    else:
                        found[key] = rel
    return found, duplicates


def new_run_dir(root, started):
    """Create this run's folder, runs/<YYYY-MM>/<UTC start>; a suffix keeps names unique."""
    name = dt.datetime.fromtimestamp(started, dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")
    month = os.path.join(root, "runs", name[:7])
    os.makedirs(month, exist_ok=True)
    for n in itertools.count(1):
        run_id = name if n == 1 else f"{name}-{n}"
        try:
            os.mkdir(os.path.join(month, run_id))
        except FileExistsError:
            continue
        return run_id, f"runs/{name[:7]}/{run_id}"


def migrate_old_layout(root, warnings):
    """Move what the old layout left (logs/<job>/<build>/ and runs/<id>.json) into
    run folders (only call while holding the lock).

    Each build goes to the folder of the run that archived it, and old run ids
    become folder names. Everything is checked before anything moves; a build
    that cannot be placed stays where it is and is reported. Safe to repeat: a
    run report remembers the id it was migrated from.
    """
    logs, runs = os.path.join(root, "logs"), os.path.join(root, "runs")
    flat = sorted(n for n in os.listdir(runs) if n.endswith(".json")) if real_dir(runs) else []
    if not real_dir(logs) and not flat:
        return
    folder_of = {}  # old run id -> new run id, from runs migrated before
    for rel in run_dirs(root):
        try:
            migrated = (read_json(os.path.join(root, rel, "run.json")) or {}).get("migrated_from")
        except (ValueError, OSError, AttributeError):
            continue
        if isinstance(migrated, str):
            folder_of[migrated] = rel.rsplit("/", 1)[1]
    reports = {}
    for name in flat:
        try:
            report = read_json(os.path.join(runs, name))
            if f"{report['run']}.json" != name or not plausible_time(report["started"], time.time()):
                raise ValueError("not this run's report")
            reports[report["run"]] = report
        except (ValueError, KeyError, TypeError, OSError) as e:
            warnings.append(f"old layout: runs/{name} cannot be moved: {type(e).__name__}: {e}")
    builds = []
    for job in sorted(listdir(logs)) if real_dir(logs) else []:
        for build in sorted(listdir(os.path.join(logs, job))) if real_dir(os.path.join(logs, job)) else []:
            src = os.path.join(logs, job, build)
            if not real_dir(src) or not JOB_RE.fullmatch(job) or not BUILD_RE.fullmatch(build):
                warnings.append(f"old layout: logs/{job[:80]}/{build[:40]} is not a build folder; left in place")
                continue
            try:
                meta = read_json(os.path.join(src, "meta.json"))
            except (ValueError, OSError):
                meta = None
            if valid_meta(meta) and (meta.get("job"), meta.get("build")) == (job, build) and \
                    isinstance(meta.get("archived_by_run"), str):
                builds.append((src, job, build, meta))
            elif real_dir(src) and all((n in LOG_FILES or n.endswith(".part")) and file_or_gone(os.path.join(src, n))
                                       for n in os.listdir(src)):
                for n in os.listdir(src):  # a download the old code never finished: fetched again
                    with contextlib.suppress(FileNotFoundError):
                        os.remove(os.path.join(src, n))
                os.rmdir(src)
            else:
                warnings.append(f"old layout: logs/{job}/{build} has no usable meta.json; left in place")

    def started_of(old):
        if old in reports:
            return reports[old]["started"]
        with contextlib.suppress(ValueError, TypeError):
            return dt.datetime.strptime(old, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=dt.UTC).timestamp()
        return None
    def folder_for(old):
        """The run folder for an old run: one an earlier, cut-off migration made, else a new one."""
        name = dt.datetime.fromtimestamp(started_of(old), dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")
        existing = os.path.join(runs, name[:7], name)
        if real_dir(existing) and name not in {v for k, v in folder_of.items() if k != v}:  # not another old run's
            try:
                report = read_json(os.path.join(existing, "run.json"))
            except (ValueError, OSError):
                report = {}
            if report is None or (isinstance(report, dict) and report.get("migrated_from") == old):
                return name
        return new_run_dir(root, started_of(old))[0]
    for *_, m in builds:
        name = m["archived_by_run"]
        if RUN_RE.fullmatch(name) and folder_time(name) is not None:  # rewritten by a migration cut off before the move
            folder_of.setdefault(name, name)
    olds = {m["archived_by_run"] for *_, m in builds} | set(reports)

    def blocked(paths):  # (nothing moves while a folder it would move into is a link, never followed in the archive,
        # or is not a folder at all)
        found = [p for p in paths if os.path.lexists(os.path.join(root, p)) and not real_dir(os.path.join(root, p))]
        if found:
            link = os.path.islink(os.path.join(root, found[0]))
            warnings.append(f"old layout: {found[0]} is {'a link' if link else 'not a folder'}, so nothing is moved into run "
                            "folders until it is fixed" + (", as it would be moved where the link leads" if link else ""))
        return bool(found)
    months = {folder_of[old][:7] if old in folder_of else dt.datetime.fromtimestamp(started_of(old), dt.UTC).strftime("%Y-%m")
              for old in olds if old in folder_of or started_of(old) is not None}
    if blocked(["runs", *(f"runs/{m}" for m in sorted(months))]):  # (before folder_for makes run folders in them)
        return
    for old in sorted(olds, key=lambda o: started_of(o) or 0):
        if old not in folder_of and started_of(old) is not None:
            folder_of[old] = folder_for(old)
    # (and the run folders and job folders: one an earlier, cut-off migration named may be either)
    rids = {old: folder_of[old] for old in olds if old in folder_of}
    if blocked(sorted({f"runs/{rid[:7]}/{rid}" for rid in rids.values()}
                     | {f"runs/{rids[m['archived_by_run']][:7]}/{rids[m['archived_by_run']]}/{job}"
                        for _, job, _, m in builds if m["archived_by_run"] in rids})):
        return
    known, _ = archived_builds(root)
    for src, job, build, meta in builds:
        old = meta["archived_by_run"]
        if old not in folder_of:
            warnings.append(f"old layout: logs/{job}/{build} names a run {old[:40]!r} with no known time; left in place")
            continue
        rid = folder_of[old]
        dest = os.path.join(runs, rid[:7], rid, job, build)
        if os.path.islink(dest):  # (a link in the archive is never followed)
            warnings.append(f"old layout: logs/{job}/{build}: {os.path.relpath(dest, root)} is a link; left in place")
            continue
        if f"{job}/{build}" in known:
            warnings.append(f"old layout: logs/{job}/{build} is also archived at {known[f'{job}/{build}']}; left in place")
            continue
        if os.path.lexists(dest):  # (no archived copy: not a folder, or one without a meta.json)
            warnings.append(f"old layout: logs/{job}/{build}: {os.path.relpath(dest, root)} is taken by what is not an "
                            "archived copy of it; left in place")
            continue
        # meta.json first, then the move: a migration cut off in between finds the build
        # still under logs/, already naming its folder, and moves it on the next run
        meta.update(archived_by_run=rid, archived_at=meta.get("archived_at") or started_of(old))
        for k in ("log_recovered_by_run", "repaired_by_run"):
            if isinstance(meta.get(k), str) and meta[k] in folder_of:
                meta[k] = folder_of[meta[k]]
        write_json(os.path.join(src, "meta.json"), meta)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        os.rename(src, dest)
    for old, report in reports.items():
        rid = folder_of[old]
        report.update(run=rid, dir=f"runs/{rid[:7]}/{rid}", migrated_from=old)
        write_json(os.path.join(runs, rid[:7], rid, "run.json"), report)
        os.remove(os.path.join(runs, f"{old}.json"))
    for job in os.listdir(logs) if real_dir(logs) else []:
        remove_if_empty(os.path.join(logs, job))
    remove_if_empty(logs)
    if real_dir(logs):
        warnings.append("old layout: some of logs/ could not be moved into run folders (see above)")


def clean_partial_builds(root, warnings):
    """Remove build folders a killed run left without a meta.json, stored files a copy
    cut off part-way left that their meta.json does not list, and run folders left
    empty (only call while holding the lock).

    Only files a download writes are removed; a folder holding anything else is reported, not touched.
    """
    def downloaded(target, n):  # (a file a download writes: one of such a name, not a folder or link)
        return bool(n in LOG_FILES or PART_RE.fullmatch(n) or (n.startswith(".") and n.endswith(".part"))) \
            and file_or_gone(os.path.join(target, n))
    for run_rel in run_dirs(root):
        run_abs = os.path.join(root, run_rel)
        for job in listdir(run_abs):
            job_abs = os.path.join(run_abs, job)
            # (only folders named as a download names them, as archived_builds reads them)
            if job in RUN_FILES or not JOB_RE.fullmatch(job) or not real_dir(job_abs):
                continue
            for build in listdir(job_abs):
                target = os.path.join(job_abs, build)
                if not BUILD_RE.fullmatch(build) or not real_dir(target):
                    continue
                try:
                    os.listdir(target)
                except (FileNotFoundError, NotADirectoryError):
                    continue
                except OSError as e:  # (what is in it cannot be told)
                    warnings.append(f"{run_rel}/{job}/{build}: cannot be listed ({type(e).__name__}); nothing in the "
                                    "folder is removed")
                    continue
                if os.path.islink(os.path.join(target, "meta.json")):  # (a link in the archive is never followed)
                    warnings.append(f"{run_rel}/{job}/{build}: its meta.json is a link; nothing in the folder is removed")
                    continue
                if os.path.exists(os.path.join(target, "meta.json")):
                    if not os.path.isfile(os.path.join(target, "meta.json")):  # (the index reads only a file)
                        warnings.append(f"{run_rel}/{job}/{build}: its meta.json is not a file; nothing in the folder "
                                        "is removed")
                        continue
                    try:
                        meta = read_json(os.path.join(target, "meta.json"))
                    except (ValueError, OSError):
                        continue  # reported as unreadable by the index
                    # (against this build's own record only: another build's says nothing of what the folder's
                    # files are, as the index reports)
                    own = valid_meta(meta) and meta.get("job") == job and meta.get("build") == build
                    for n in stray_files(target, meta) if own else []:
                        if not file_or_gone(os.path.join(target, n)):  # (not what a copy cut off part-way leaves)
                            warnings.append(f"{run_rel}/{job}/{build}/{n}: not listed in its meta.json, and not a file "
                                            "a download writes; left alone")
                            continue
                        try:
                            remove(os.path.join(target, n))
                            warnings.append(f"{run_rel}/{job}/{build}/{n}: removed, as its meta.json does not list "
                                            "it (a copy cut off part-way)")
                        except OSError as e:
                            warnings.append(f"{run_rel}/{job}/{build}/{n}: not listed in its meta.json, and "
                                            f"cannot be removed: {e}")
                    continue
                names = listdir(target)
                if all(downloaded(target, n) for n in names):
                    for n in names:
                        remove(os.path.join(target, n))
                    remove_build_dir(target)
                else:
                    warnings.append(f"{run_rel}/{job}/{build}: no meta.json but unexpected files {sorted(names)[:5]}")
            if not listdir(job_abs):
                remove_if_empty(job_abs)
        # (empty: a run killed right after it made its folder, or, with code from before the README from
        # the start, before it archived or reported anything)
        if not listdir(run_abs):
            remove_if_empty(run_abs)
            remove_if_empty(os.path.dirname(run_abs))


INDEX_TESTS = 100  # red tests an index entry lists (meta.json lists more, but past MAX_RED_CELLS in a table counts the cells)


def index_entry(job, build, meta, now, rel=None):
    t = meta["started"] or meta["created"]
    if not plausible_time(t, now):
        raise ValueError(f"implausible time {t!r}")
    red_tests = sorted({c["test"] for tab in meta["tabs"] for c in tab["red_cells"]},
                       key=lambda name: (name == f"{job}.Overall", name.startswith(f"{job}."), name))
    job_rows = [name for name in red_tests if name.startswith(f"{job}.")]
    left_out = sum(cells_left_out(tab) for tab in meta["tabs"])
    files = [((meta.get(key) or {}).get("file", name), meta.get(key))
             for name, key in (("build-log.txt", "log"), ("podinfo.json", "podinfo"))]
    base = (meta.get("log_url") or "").rsplit("/", 1)[0]
    remote = [(name.removesuffix(".gz") if name else default, f"{base}/{default}")
              for (name, info), default in zip(files, ("build-log.txt", "podinfo.json"), strict=True)
              if info and info.get("omitted") and base]
    since = failing_since(meta, now)
    return {
        "time": t,
        "job": job,
        "build": build,
        "result": meta.get("result") if is_text(meta.get("result"), 40) else None,
        "tabs": [f"{t['dashboard']}#{t['tab']}" for t in meta["tabs"]],
        # specific failing tests first, then job-level rows such as .Pod, .Overall last (at most
        # INDEX_TESTS of them: a mass failure would make each week's page huge)
        "red_tests": red_tests[:max(0, INDEX_TESTS - len(job_rows))] + job_rows if len(red_tests) > INDEX_TESTS
        else red_tests,
        "red_test_count": len(red_tests),
        # (red cells meta.json counts but does not list: cells, not tests, as one test can be red on several tabs)
        **({"red_cells_omitted": left_out} if left_out else {}),
        "dir": rel,
        "run": rel.split("/")[2] if rel else None,
        "files": [f"{rel}/{part['file']}" for name, info in files if info and not info.get("omitted")
                  for part in info.get("parts") or [{"file": name}]] if rel else [],
        "remote": remote,
        "bytes": sum(info["bytes"] for _, info in files if info),
        "settled": meta.get("settled") if isinstance(meta.get("settled"), bool) else None,
        "final": meta.get("final") if isinstance(meta.get("final"), bool) else None,
        # (since when its comparisons with GCS have failed, if they have, and whether the runs gave up on it)
        **({"comparison_failing_since": since} if since is not None else {}),
        **({"comparison_given_up": True} if given_up_on(meta) else {}),
        "backfilled": bool(meta.get("backfilled")),
        "has_log": bool(meta.get("log")),
        "in_parts": any((meta.get(key) or {}).get("parts") for key in ("log", "podinfo")),
        # red only by TestGrid's rule applied to GCS: meta.json keeps no sighting of it on TestGrid (see merge_tabs)
        "from_gcs": bool(meta.get("backfilled")) and all(
            t.get("judged_from_gcs") or not (t.get("red_cells") or cells_left_out(t)) for t in meta["tabs"]),
        "prow_url": meta.get("prow_url") or f"https://prow.k8s.io/view/gs/{meta['query']}/{build}",
    }


def load_index(root, now, known=None):
    """Every readable meta.json as an index entry, plus a list of unreadable ones."""
    entries, bad = [], []
    if known is None:
        known, _ = archived_builds(root)  # duplicates are reported by run() and cross_check
    for key, rel in sorted(known.items()):
        job, build = key.split("/", 1)
        try:
            meta = read_json(os.path.join(root, rel, "meta.json"))
            if not valid_meta(meta):
                raise ValueError("not an archive meta.json")
            if meta.get("job") != job or meta.get("build") != build:
                raise ValueError(f"describes {str(meta.get('job'))[:80]}/{str(meta.get('build'))[:40]}")
            entries.append(index_entry(job, build, meta, now, rel))
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError, OSError) as e:
            bad.append(f"{rel}/meta.json: {type(e).__name__}: {e}")
    entries.sort(key=lambda e: (e["time"], e["build"]), reverse=True)
    return entries, bad


# What can start markup inside a table cell or list item: emphasis, code, links and
# images, raw HTML, a table pipe, strikethrough. Block markers only matter at the start.
MD_INLINE = re.compile(r"([\\`*_\[\]<>|!~])")


def md_cell(s):
    """Text safe inside a Markdown table cell or list item: one line, no markup, links, images or HTML."""
    s = MD_INLINE.sub(r"\\\1", re.sub(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]+", " ", str(s)).strip())
    if s and s[0] in "#+-=":
        return "\\" + s
    return re.sub(r"^([0-9]+)([.)])", r"\1\\\2", s)


def build_rows(entries, files=True, prefix=""):
    lines, day = [], None
    for e in entries:
        d = iso(e["time"])[:10]
        if d != day:
            day = d
            head = "| Time | Tab | Build | Red tests | " + ("Files |" if files else "Status |")
            lines += ["", f"### {day}", "", head, "|---|---|---|---|---|"]
        tests, count = e["red_tests"], e.get("red_test_count", len(e["red_tests"]))
        shown = (md_cell(tests[0][:90]) + (f" (+{count - 1})" if count > 1 else "")) if tests else ""
        if e.get("red_cells_omitted"):
            n = e["red_cells_omitted"]
            shown = (shown + " (and " if shown else "(") + f"{n} red cell{'s' if n != 1 else ''} left out)"
        if e.get("from_gcs"):
            shown = (shown + " " if shown else "") + "(taken from GCS: red by TestGrid's rule, not seen red on TestGrid)"
        if files:
            links = [f"[{p.rsplit('/', 1)[1]}]({prefix}{p})" for p in e["files"]]
            links += [f"[{name} in GCS (too large to store)]({url})" for name, url in e.get("remote") or []]
            last = " ".join(links) or "none"
            if e.get("comparison_given_up"):  # (final for that, settled or not)
                since = e.get("comparison_failing_since")
                last += f" (no comparison with GCS has succeeded since {iso(since)[:10]})" if since else \
                    " (its comparisons with GCS failed)"
            elif e.get("settled") is False:
                last += " (never settled: finished.json did not appear)" if e.get("final") else " (provisional)"
        else:
            last = md_cell(e["status"])
        tabs = "<br>".join(md_cell(t.replace("sig-release-master-", "")) for t in e["tabs"])
        lines.append(f"| {iso(e['time'])[11:16]} | {tabs} | [{e['job']} {e['build']}]({e['prow_url']}) | {shown} | {last} |")
    return lines


def week_of(t):
    year, week, _ = dt.datetime.fromtimestamp(t, dt.UTC).isocalendar()
    return f"{year}-W{week:02d}"


def week_days(key):
    year, week = key.split("-W")
    monday = dt.date.fromisocalendar(int(year), int(week), 1)
    return monday, monday + dt.timedelta(days=6)


def short(d):
    return f"{d:%b} {d.day}"


def write_text(path, text):
    """Write path atomically, leaving it untouched when the content is the same (a link there is
    replaced, not read)."""
    try:
        if not os.path.islink(path):
            with open(path, encoding="utf-8") as f:
                if f.read() == text:
                    return
    except (FileNotFoundError, UnicodeDecodeError):
        pass
    write_file(path, text)


def run_title(name):
    """"2026-10-09T001742Z-2" -> "2026-10-09 00:17:42 (2)"."""
    t = f"{name[:10]} {name[11:13]}:{name[13:15]}:{name[15:17]}"
    return t + (f" ({name[19:]})" if len(name) > 18 else "")


def run_counts(run):
    """(new builds, updated builds, problems) from a run report, tolerating malformed ones."""
    outcomes = run.get("outcomes") if isinstance(run.get("outcomes"), dict) else {}

    def n(*keys):
        return sum(len(v) for k in keys if isinstance(v := outcomes.get(k), list))
    tabs = run.get("tabs") if isinstance(run.get("tabs"), list) else []
    errors = run.get("errors") if isinstance(run.get("errors"), list) else []
    serious = run.get("serious_warnings") if isinstance(run.get("serious_warnings"), list) else []
    problems = len(errors) + len(serious) + sum(1 for t in tabs if isinstance(t, dict) and t.get("error"))
    return n("archived", "archived-without-log", "backfilled"), n("repaired", "refreshed", "log-recovered"), problems


def tabs_read_text(reports):
    """"N tabs read (M failed)" for a run's tab reports, then ", K dashboard summary problems" for
    those without a tab (a dashboard's summary that could not be read, or a tab name in it that
    cannot be used), if any."""
    tabs = [r for r in reports if isinstance(r, dict) and r.get("tab") is not None]
    other = sum(1 for r in reports if isinstance(r, dict) and r.get("tab") is None and r.get("error"))
    text = f"{len(tabs)} tabs read ({sum(1 for r in tabs if r.get('error'))} failed)"
    return text + (f", {other} dashboard summary " + ("problem" if other == 1 else "problems") if other else "")


def problems_text(n):
    """"1 problem", "2 problems"."""
    return f"{n} problem" + ("" if n == 1 else "s")


def run_rows(root, first_day, current=None):
    """Markdown rows for the run folders started on or after first_day, newest first."""
    rows = []
    for rel in reversed(run_dirs(root)):
        name = rel.rsplit("/", 1)[1]
        if dt.date.fromisoformat(name[:10]) < first_day:
            break
        run = current if current and current.get("dir") == rel else read_report(root, rel)
        verified = "pending" if current and current.get("dir") == rel else verification_of(root, rel)
        if not isinstance(run, dict):
            result = "unreadable run report" if os.path.lexists(os.path.join(root, rel, "run.json")) else \
                "no run report: the run was cut off"
            rows.append(f"| [{run_title(name)}]({rel}/) | | | {result} | {verified} |")
            continue
        new, updated, problems = run_counts(run)
        result = "interrupted" if run.get("interrupted") is True else problems_text(problems) if problems else "ok"
        rows.append(f"| [{run_title(name)}]({rel}/) | {new} | {updated} | {result} | {verified} |")
    return rows


def verification_of(root, rel):
    """A run's Verification cell in INDEX.md: the outcome its verification.json records, linked."""
    path = os.path.join(root, rel, "verification.json")
    if not os.path.lexists(path):
        return "not recorded"
    try:
        v = None if os.path.islink(path) else read_json(path)  # (a link in the archive is never followed)
    except (ValueError, OSError):
        v = None
    outcome = v.get("outcome") if isinstance(v, dict) else None
    if outcome not in VERIFICATION_OUTCOMES:
        return "unreadable"
    if outcome == "problems" and is_int(n := v.get("problem_count")) and n > 0:
        outcome = problems_text(n)
    return f"[{outcome}]({rel}/verification.json)"


def show_verifications(root):
    """Put each run's verification, as its verification.json records it now, in INDEX.md's table of
    runs (the fetch writes the table before the run it makes is verified: "pending"), and change
    nothing else: only the rows of run folders run_dirs lists (a link in the archive is never
    followed). Returns how many rows changed."""
    path = os.path.join(root, "INDEX.md")
    if os.path.islink(path):  # (a link in the archive is never followed: the next fetch writes INDEX.md)
        return 0
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.read().split("\n")
    except FileNotFoundError:
        return 0
    changed, table, listed = 0, False, set(run_dirs(root))
    for i, line in enumerate(lines):
        if line.startswith("## "):
            table = line.startswith("## Runs in the last ")
        m = re.match(r"\| \[[^\]]*\]\((runs/[0-9]{4}-[0-9]{2}/[^/)]+)/\) \| ", line) if table else None
        cut = line.rfind(" | ", 0, len(line) - 2)
        if m and m.group(1) in listed and line.endswith(" |") and cut > m.end() - 4:
            new = line[:cut + 3] + verification_of(root, m.group(1)) + " |"
            if new != line:
                lines[i], changed = new, changed + 1
    if changed:
        write_text(path, "\n".join(lines))
    return changed


def write_run_readme(root, rel, run, entries):
    """README.md of a run folder: what the run did, and the builds it archived, with links."""
    name = rel.rsplit("/", 1)[1]
    up = "../../../"
    mine = [e for e in entries if (e.get("dir") or "").startswith(rel + "/")]
    lines = [f"# Run {run_title(name)} UTC", ""]
    if isinstance(run, dict):
        tabs = run.get("tabs") if isinstance(run.get("tabs"), list) else []
        new, updated, problems = run_counts(run)
        seen = run.get("red_builds_seen") if is_int(run.get("red_builds_seen")) else "?"
        lines.append(f"{tabs_read_text(tabs)}, {seen} red builds on "
                     f"TestGrid. This run archived {new} new builds" + (f" and updated {updated} archived earlier"
                                                                       if updated else "") + ".")
        if run.get("interrupted") is True:
            lines.append("The run was interrupted; unfinished builds are retried by the next run.")
        read = run.get("testgrid_config")
        source = read.get("source") if isinstance(read, dict) else None
        if isinstance(source, dict) and isinstance(source.get("test_infra"), str) and \
                plausible_time(read.get("read"), time.time()):
            when = "by this run" if read["read"] == run.get("started") else f"by the run of {iso(read['read'])}"
            lines.append(f"TestGrid's config: {md_cell(config_about(source))}, read {when}.")
    elif os.path.lexists(os.path.join(root, rel, "run.json")):
        lines.append("Its report, [run.json](run.json), cannot be read.")
    else:
        lines.append("This run was cut off before it wrote its report.")
    details = "[run.json](run.json) has the details; " if isinstance(run, dict) else ""
    lines += ["", f"{details}[INDEX.md]({up}INDEX.md) lists the archived builds of the last {report.HEAT_DAYS} days, "
              "and links a page for each week. "
              "A file marked provisional may have been taken before Prow finished uploading; it is compared "
              "with GCS on every run and fetched again if GCS changes.", ""]
    lines.append(f"## Builds archived by this run ({len(mine)})")
    lines += build_rows(mine, prefix=up) if mine else ["", "None."]
    if isinstance(run, dict):
        outcomes = run.get("outcomes") if isinstance(run.get("outcomes"), dict) else {}
        elsewhere = [(k, key) for k in ("repaired", "refreshed", "log-recovered")
                     for key in (outcomes.get(k) if isinstance(outcomes.get(k), list) else []) if isinstance(key, str)]
        where = {f"{e['job']}/{e['build']}": e.get("dir") for e in entries}
        if elsewhere:
            lines += ["", "## Archived earlier, updated by this run", ""]
            lines += [f"- {k}: [{md_cell(key)}]({up}{where[key]}/)" if where.get(key) else f"- {k}: {md_cell(key)}"
                      for k, key in elsewhere]
        # (a dashboard's summary that could not be read, or a tab name in it that cannot be used, has
        # no tab; an error of the whole run has no build)
        problems = [f"{md_cell(t.get('dashboard'))}{'' if t.get('tab') is None else '#' + md_cell(t['tab'])}: "
                    f"{md_cell(t['error'])}"
                    for t in (run.get("tabs") if isinstance(run.get("tabs"), list) else [])
                    if isinstance(t, dict) and t.get("error")]
        problems += [("" if e.get("build") is None else f"{md_cell(e['build'])}: ") + md_cell(e.get("error"))
                     for e in (run.get("errors") if isinstance(run.get("errors"), list) else []) if isinstance(e, dict)]
        problems += [md_cell(w) for w in (run.get("serious_warnings") if isinstance(run.get("serious_warnings"), list)
                                          else [])]
        if problems:
            lines += ["", "## Problems", ""] + [f"- {p}" for p in problems]
        serious = run.get("serious_warnings") if isinstance(run.get("serious_warnings"), list) else []
        notes = [md_cell(w) for w in (run.get("warnings") if isinstance(run.get("warnings"), list) else [])
                 if w not in serious]
        if notes:
            lines += ["", "## Warnings", ""] + [f"- {w}" for w in notes]
    write_text(os.path.join(root, rel, "README.md"), "\n".join(lines) + "\n")


def holdings(entries, waiting, current_run):
    """INDEX.md's count of what the archive holds, by kind (cross_check prints the same)."""
    def count(test):
        return sum(1 for e in entries if test(e))
    watched = (current_run or {}).get("watched")
    watched = watched if is_int(watched) else len(((current_run or {}).get("outcomes") or {}).get("watching") or [])
    rows = [("Archived builds", len(entries)),
            ("… with a log", count(lambda e: e["has_log"])),
            ("… with only podinfo.json (the pod never ran)", count(lambda e: not e["has_log"] and e["files"])),
            ("… without a file (GCS had none)", count(lambda e: not e["has_log"] and not e["files"] and not e["remote"])),
            ("… with a file stored in parts (too large for one file in git)", count(lambda e: e["in_parts"])),
            ("… provisional (perhaps taken before Prow finished uploading)",
             count(lambda e: e["settled"] is False and e["final"] is not True)),
            ("… still compared with GCS on every run", count(lambda e: e["final"] is not True)),
            ("… given up on after failed comparisons with GCS (still compared at least once a week)",
             count(lambda e: "comparison_given_up" in e)),
            ("… taken from GCS, judged by TestGrid's rule", count(lambda e: e["backfilled"])),
            ("Red builds waiting to be archived", sum(1 for e in waiting if not e["status"].startswith("given up"))),
            ("… of them with a damaged copy here, to be fetched again",
             sum(1 for e in waiting if e["on_disk"] and not e["status"].startswith("given up"))),
            ("Red builds given up on (after 14 days)", sum(1 for e in waiting if e["status"].startswith("given up"))),
            ("Builds watched until TestGrid's rule can judge them (still running, unreadable, or shown under "
             "a config not known or followed here)", watched)]
    return (["## What the archive holds", "", "| | Builds |", "|---|---:|"]
            + [f"| {md_cell(what)} | {n} |" for what, n in rows])


def left_for_later(left):
    """INDEX.md's list of what later runs still have to search or judge, from a run report's
    "left_for_later" (see run()): none, if there is nothing."""
    def part(name):
        value = left.get(name) if isinstance(left, dict) else None
        return value if isinstance(value, dict) else {}
    gaps, ranges, unlisted = part("gap_parts"), part("config_ranges"), part("unlisted")

    def again(key):  # (a tab no longer listed is not read, and is forgotten UNLISTED_DAYS after that)
        return f"if TestGrid lists the tab again by {iso(unlisted[key] + UNLISTED_DAYS * 86400)}"
    lines = [f"- {md_cell(key)}: the older part of its history gap, from {iso(lo)} to {iso(hi)}, is searched in GCS "
             + (again(key) if key in unlisted else "by the next runs") for key, (lo, hi) in sorted(gaps.items())]
    lines += [f"- {md_cell(key)}: {r['ranges']} range{'' if r['ranges'] == 1 else 's'} of builds a scan saw, waiting "
              f"for a config of the tab not known yet (the oldest scan {iso(r['oldest_scan'])}); "
              + (f"looked at again {again(key)}" if key in unlisted else f"each is dropped {GIVE_UP_DAYS} days after its scan")
              for key, r in sorted(ranges.items())]
    return ["## Left for later runs", ""] + lines + [""] if lines else []


def write_index(root, now, current_run=None, missing=(), known=None):
    """Rewrite INDEX.md, the weekly pages, the charts and recent runs' READMEs.

    `missing` are red builds not archived (yet); `current_run` is this run's
    report so far, with "dir" set to its folder. Returns (entries, unreadable).
    """
    entries, bad = load_index(root, now, known)
    if current_run is not None and isinstance(current_run.get("errors"), list):
        current_run["errors"] += [{"build": None, "error": f"unreadable {b}"} for b in bad]
    weeks = {}
    for e in entries:
        weeks.setdefault(week_of(e["time"]), []).append(e)
    if os.path.islink(os.path.join(root, "weeks")):  # (a link in the archive is never followed: a folder replaces it)
        os.remove(os.path.join(root, "weeks"))
    os.makedirs(os.path.join(root, "weeks"), exist_ok=True)
    listing = []
    for key in sorted(weeks, reverse=True):
        first, last = week_days(key)
        items = weeks[key]
        page = [f"# Red TestGrid builds, week {key}", "",
                f"{short(first)} – {short(last)}, {last.year} (UTC): {len(items)} builds, newest first. "
                f"[Back to the index](../INDEX.md)."]
        write_text(os.path.join(root, "weeks", f"{key}.md"), "\n".join(page + build_rows(items, prefix="../")) + "\n")
        write_text(os.path.join(root, "weeks", f"{key}.json"), json.dumps(items, indent=2, ensure_ascii=False) + "\n")
        listing.append({"week": key, "from": first.isoformat(), "to": last.isoformat(), "builds": len(items),
                        "page": f"weeks/{key}.md", "data": f"weeks/{key}.json"})
    write_text(os.path.join(root, "index.json"), json.dumps(listing, indent=2) + "\n")
    for name in os.listdir(os.path.join(root, "weeks")):
        m = re.fullmatch(r"(\d{4}-W\d{2})\.(md|json)", name)
        if m and m.group(1) not in weeks:
            os.remove(os.path.join(root, "weeks", name))

    waiting, archived = [], {(e["job"], e["build"]) for e in entries}
    for m in missing:
        try:
            entry = index_entry(m["rec"]["job"], m["rec"]["build"], dict(m["rec"], result=None), now)
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
            continue
        entry["status"] = m["status"]
        entry["on_disk"] = (entry["job"], entry["build"]) in archived
        waiting.append(entry)
    waiting.sort(key=lambda e: (e["time"], e["build"]), reverse=True)
    # A build with a damaged copy on disk is listed as waiting but counted once, as archived.
    counted = entries + [e for e in waiting if not e["on_disk"]]
    first_day = report.day_of(now) - dt.timedelta(days=report.HEAT_DAYS - 1)
    recent = [e for e in entries if report.day_of(e["time"]) >= first_day]
    lines = ["# Red TestGrid builds", "",
             f"Updated {iso(now)}. The builds with a red cell on {' and '.join(DASHBOARDS)} that the runs "
             "found: seen red on TestGrid, or taken from GCS by TestGrid's rule ([what that can miss or add]"
             "(../README.md#limits)); times are UTC. Each archived build is in the folder of the run that "
             "first archived it, under [runs/](runs/). A file marked provisional may have been taken before "
             "Prow finished uploading; it is compared with GCS on every run and fetched again if GCS changes.", ""]
    lines += report.write_charts(root, counted, now, current_run, run_dirs(root))
    if waiting:
        lines += ["## Not archived yet", "", f"{len(waiting)} red builds without an intact local copy yet."]
        lines += build_rows(waiting, files=False) + [""]
    lines += left_for_later((current_run or {}).get("left_for_later"))
    lines += [f"## Runs in the last {report.HEAT_DAYS} days", "",
              "Collection result is how the run's fetch went: ok, interrupted, its number of problems (errors, "
              "serious warnings, and the tabs and dashboard summaries it could not read, listed in the README of "
              "the run's folder and in its run.json), no run report if the run was cut off before writing one, or "
              "unreadable run report if its run.json cannot be read. It does not include the verification that "
              "follows the fetch ([cross_check.py](../tool/cross_check.py)).", "",
              "Verification is the outcome that verification recorded in the run's verification.json: ok, its "
              "number of problems, timed out, interrupted, stopped (by an error), or incomplete (it ended "
              "without recording one). It is pending for the run that wrote this page until its verification ends, when "
              "the workflow puts the outcome here; not "
              "recorded when the run has no verification.json (it ran before verifications were recorded, was not "
              "verified, or none could be written); and unreadable when the file is damaged.", "",
              "| Run (UTC) | New builds | Updated | Collection result | Verification |", "|---|---:|---:|---|---|"]
    lines += run_rows(root, first_day, current_run) + [""]
    lines += [f"## Builds from the last {report.HEAT_DAYS} days", "",
              f"{len(recent)} archived builds that started since {first_day.isoformat()} (UTC), newest first."]
    lines += build_rows(recent) + [""] + holdings(entries, waiting, current_run) + ["", "## Every week", ""]
    lines += [f"- [{w['week']}]({w['page']}): {short(dt.date.fromisoformat(w['from']))} – "
              f"{short(dt.date.fromisoformat(w['to']))}, {w['builds']} builds" for w in listing]
    write_text(os.path.join(root, "INDEX.md"), "\n".join(lines) + "\n")
    # Recent runs' READMEs show their builds' current state (a provisional file can settle later), and,
    # whatever their age, those of runs cut off (with no report, or the one a run has until it ends)
    # what they archived (see run()), and those whose builds now have other files than they link (a
    # later run added a podinfo.json, or took a file again in another form: see recheck); a run folder
    # without one gets one.
    horizon = now - RECHECK_HOURS * 3600 - 86400
    for rel in run_dirs(root):
        if current_run and current_run.get("dir") == rel:
            write_run_readme(root, rel, current_run, entries)
            continue
        run = read_report(root, rel)
        if folder_time(rel.rsplit("/", 1)[1]) >= horizon or not os.path.lexists(os.path.join(root, rel, "run.json")) \
                or stopped(run) or not os.path.lexists(os.path.join(root, rel, "README.md")) \
                or links_other_files(root, rel, entries):
            write_run_readme(root, rel, run, entries)
    return entries, bad


def links_other_files(root, rel, entries):
    """Whether the README.md of a run folder links other files of its builds than they have (see
    write_run_readme: the files of each, by its row). Not one that is a link, which is never followed
    (as in linking()), nor one that cannot be read: those stay as they are."""
    path = os.path.join(root, rel, "README.md")
    try:
        if os.path.islink(path):
            return False
        with open(path, "rb") as f:
            text = f.read().decode("utf-8", "replace")
    except OSError:
        return False
    linked = set(re.findall(re.escape(f"](../../../{rel}/") + r"([^()\s]*[^()\s/])\)", text))
    return linked != {p.removeprefix(rel + "/") for e in entries if (e.get("dir") or "").startswith(rel + "/")
                      for p in e["files"]}


def lock_archive(root):
    """Hold an exclusive lock on the archive so two runs never write it at once."""
    os.makedirs(root, exist_ok=True)
    f = open(os.path.join(root, ".lock"), "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.close()
        return None
    return f


EXCLUDED = "excluded.json"  # in the archive: builds taken out of it for good (see load_excluded)


class ExcludedUnreadable(Exception):
    """excluded.json is there but cannot be used: no run goes on, as it could fetch a build taken out."""


def load_excluded(root):
    """The builds the archive's excluded.json takes out for good, {"job/build": {"reason": why}},
    edited by hand (such as for a log that must not stay public): each run removes their folders
    and never fetches them again, and cross_check.py does not count them as missing. Raises
    ExcludedUnreadable when the file is there but cannot be used."""
    path = os.path.join(root, EXCLUDED)
    if os.path.islink(path):  # (a link in the archive is never followed)
        raise ExcludedUnreadable(f"{path} is a link")
    try:
        data = read_json(path)
    except (OSError, ValueError) as e:
        raise ExcludedUnreadable(f"{path}: {type(e).__name__}: {e}") from None
    if data is None and not os.path.lexists(path):
        return frozenset()

    def usable(k, v):
        job, _, build = k.partition("/") if isinstance(k, str) else ("", "", "")
        return JOB_RE.fullmatch(job) and BUILD_RE.fullmatch(build) and not build.startswith("0") \
            and isinstance(v, dict) and is_text(v.get("reason"), 1000) and v["reason"].strip()  # (no build has a 0 first)
    bad = [str(k)[:80] for k, v in data.items() if not usable(k, v)] if isinstance(data, dict) else ["(not an object)"]
    if bad:
        raise ExcludedUnreadable(f'{path}: not {{"job/build": {{"reason": "why"}}}} for {", ".join(bad[:3])}')
    return frozenset(data)


def write_cut_off_readme(root, rel):
    """The README of the folder of a run cut off before it ended, as the run writes it from its start
    (see run()): every later run writes it again, with what the run archived (see write_index)."""
    write_text(os.path.join(root, rel, "README.md"),
               f"# Run {run_title(rel.rsplit('/', 1)[1])} UTC\n\nThis run was cut off before it ended. The next run "
               "lists the builds it archived here.\n")


def remove_build(target):
    """Remove a build folder, whatever is in it (a build taken out, see load_excluded), and its job
    folder if no other build is in it. Raises OSError if it cannot (rmtree never follows a link)."""
    with context().write_lock:
        refuse_if_abandoned(target)
        shutil.rmtree(target)
    remove_if_empty(os.path.dirname(target))


def linking(root, keys):
    """The run folders whose README.md lists a build of `keys` ("job/build"): links its folder,
    or the build on Prow, as its row does (a README that is a link is never followed)."""
    marks = [mark.encode() for key in keys for mark in (f"/{key}/", f"/{key})")]
    out = set()
    for rel in run_dirs(root) if keys else ():
        path = os.path.join(root, rel, "README.md")
        try:
            if os.path.islink(path) or not os.path.isfile(path):
                continue
            with open(path, "rb") as f:  # (one a hand edit left not UTF-8 too)
                data = f.read()
        except OSError:
            continue
        if any(mark in data for mark in marks):
            out.add(rel)
    return out


def load_state(root, warnings):
    path = os.path.join(root, "state.json")
    try:
        state = {} if os.path.islink(path) else read_json(path)  # (a link in the archive is never followed)
    except ValueError as e:
        warnings.append(f"state.json is corrupt ({e}); starting without it")
        state = {}
    if os.path.islink(path):
        warnings.append("state.json is a link; starting without it")
    if state is None:
        state = {}  # a new archive
    if not isinstance(state, dict):
        warnings.append(f"state.json holds a {type(state).__name__}, not an object; starting without it")
        state = {}
    for name in ("tabs", "unresolved", "given_up", "watch"):
        if name in state and not isinstance(state[name], dict):
            warnings.append(f"state.json: {name} is a {type(state[name]).__name__}, not an object; dropped it")

    now = time.time()

    def entries(name):
        value = state.get(name)
        kept = {}
        for k, v in (value.items() if isinstance(value, dict) else ()):
            if (isinstance(v, dict) and valid_rec(v.get("rec"), now) and k == f"{v['rec']['job']}/{v['rec']['build']}"
                    and plausible_time(v.get("first_seen"), now)):
                kept[k] = dict(v, last_error=v["last_error"] if is_text(v.get("last_error"), 1 << 16) else None)
            else:
                warnings.append(f"state.json: dropped a malformed {name} entry {str(k)[:80]!r}")
        return kept

    tabs = {}
    for k, v in (state.get("tabs") or {}).items() if isinstance(state.get("tabs"), dict) else ():
        if plausible_time(v, now):  # older format: just the scan time
            tabs[k] = {"scan": v, "oldest": None}
        elif isinstance(v, dict) and plausible_time(v.get("scan"), now) and \
                (v.get("oldest") is None or plausible_time(v["oldest"], now)) and \
                (v.get("missing_since") is None or plausible_time(v["missing_since"], now)):
            tabs[k] = {key: v[key] for key in ("scan", "oldest", "missing_since") if v.get(key) is not None}
            tabs[k].setdefault("oldest", None)
            if known_rows(v.get("rows")):
                tabs[k]["rows"] = v["rows"]
            if isinstance(v.get("query"), str) and QUERY_RE.fullmatch(v["query"]):
                tabs[k]["query"] = v["query"]
            for name in ("current_job_hours", "job_hours", "margin"):  # checked again where they are used
                if name in v:
                    tabs[k][name] = v[name]
            older = v.get("unsearched")  # (the part of a history gap not searched yet, see run())
            if isinstance(older, list) and len(older) == 2 and all(plausible_time(t, now) for t in older) \
                    and older[0] < older[1]:
                tabs[k]["unsearched"] = older
            # (builds a scan saw that the tab no longer shows, while a config not known yet was in force: see run())
            waiting = [p for p in v.get("changed_pending") or () if isinstance(p, dict) and set(p) == {"since", "lo", "hi"}
                       and all(plausible_time(p[x], now) for x in p) and p["lo"] < p["hi"]] \
                if isinstance(v.get("changed_pending"), list) else []
            if waiting:
                tabs[k]["changed_pending"] = waiting
        else:
            warnings.append(f"state.json: dropped a malformed tabs entry {str(k)[:80]!r}")
    # each tab's TestGrid config: {"config": entry, "read": when, "mismatch": record}, kept apart from
    # its scans (a tab can lose those, or be forgotten, while builds of it are still judged)
    configs = {}
    value = state.get("tab_configs")
    if value is not None and not isinstance(value, dict):
        warnings.append("state.json: tab_configs is not an object; dropped it")
    for k, v in (value.items() if isinstance(value, dict) else ()):
        if not isinstance(v, dict) or not set(v) <= {"config", "read", "mismatch"}:
            warnings.append(f"state.json: dropped a malformed tab_configs entry {str(k)[:80]!r}")
            continue
        kept = {}
        if "config" in v:
            if tgc.valid_entry(v["config"]):
                kept["config"] = v["config"]
                if plausible_time(v.get("read"), now):
                    kept["read"] = v["read"]
            else:
                warnings.append(f"state.json: dropped the malformed TestGrid config of {str(k)[:80]!r}")
        m = v.get("mismatch")
        if m is not None:
            if isinstance(m, dict) and set(m) == {"detail", "since", "until"} and is_text(m["detail"], 4096) \
                    and plausible_time(m["since"], now) \
                    and (m["until"] is None or plausible_time(m["until"], now) and m["until"] >= m["since"]):
                kept["mismatch"] = m
            else:
                warnings.append(f"state.json: dropped a malformed config mismatch record of {str(k)[:80]!r}")
        if kept:
            configs[k] = kept
    read = state.get("testgrid_config")
    if read is not None and not (isinstance(read, dict) and plausible_time(read.get("read"), now)
                                 and isinstance(read.get("source"), dict)
                                 and isinstance(read["source"].get("test_infra"), str)
                                 and all(read["source"].get(k) is None or plausible_time(read["source"][k], now)
                                         for k in ("committed", "uploaded"))):
        warnings.append("state.json: dropped a malformed testgrid_config record")
        read = None
    # tabs TestGrid listed while the latest config known lacked them: when it last did (see run())
    lacked = state.get("lacking_tabs")
    if lacked is not None and not (isinstance(lacked, dict) and all(isinstance(k, str) and plausible_time(v, now)
                                                                    for k, v in lacked.items())):
        warnings.append("state.json: dropped a malformed lacking_tabs record")
        lacked = None
    # Older archives also kept "no_log" here; the builds' own meta.json now says what to re-check.
    # (ConfigHistory checks the config history.)
    return {"tabs": tabs, "unresolved": entries("unresolved"), "given_up": entries("given_up"),
            "watch": entries("watch"), "tab_configs": configs, **({"testgrid_config": read} if read else {}),
            "config_uploads": state.get("config_uploads"), "config_entries": state.get("config_entries"),
            "config_relist_after": state.get("config_relist_after"), **({"lacking_tabs": lacked} if lacked else {})}


def valid_rec(rec, now):
    """A build record from state.json that is safe to turn into paths and URLs."""
    try:
        return (isinstance(rec["job"], str) and JOB_RE.fullmatch(rec["job"]) is not None
                and isinstance(rec["build"], str) and BUILD_RE.fullmatch(rec["build"]) is not None
                and isinstance(rec["query"], str) and "," not in rec["query"]
                and rec["query"].rsplit("/", 1)[-1] == rec["job"]
                and (rec["started"] is None or plausible_time(rec["started"], now))
                and plausible_time(rec["created"], now)
                and isinstance(rec["tabs"], list)
                and all(isinstance(t, dict) and isinstance(t.get("red_cells"), list)
                        and "dashboard" in t and "tab" in t for t in rec["tabs"]))
    except (KeyError, TypeError):
        return False


def positive(kind):
    def parse(text):
        try:
            value = kind(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not a number: {text!r}") from None
        if not math.isfinite(value) or value <= 0:
            raise argparse.ArgumentTypeError(f"must be a positive number, got {text!r}")
        return value
    return parse


def sweep_parts(root):
    """Remove temp files a killed run left behind (only call while holding the lock).

    Symlinked folders are not entered, so nothing outside the archive is touched.
    """
    removed = 0
    for folder, _, names in os.walk(root, followlinks=False):
        in_charts = os.path.relpath(folder, root) == "charts"
        for name in names:
            path = os.path.join(folder, name)
            if name.endswith(".part") and (name.startswith(".") or in_charts) and not os.path.islink(path):
                with contextlib.suppress(OSError):
                    os.remove(path)
                    removed += 1
    return removed


def real_dir(path):
    """A directory that is not a symlink (symlinks in the archive are never followed)."""
    return os.path.isdir(path) and not os.path.islink(path)


def file_or_gone(path):
    """Neither a folder nor a link: a file, as a download writes, or nothing any more (one that went
    while it was looked at, as a temp file a cut-off worker removes). One that cannot be looked at is
    neither."""
    try:
        mode = os.lstat(path).st_mode
    except (FileNotFoundError, NotADirectoryError):
        return True
    except OSError:
        return False
    return not stat.S_ISDIR(mode) and not stat.S_ISLNK(mode)


def archived_intact(target, deep=False):
    try:
        meta = read_json(os.path.join(target, "meta.json"))
    except (ValueError, OSError):
        return False
    return valid_meta(meta) and intact(target, meta, deep)


def updated_by(meta, run_id):
    """How run_id updated a build archived earlier, as its meta.json records: "repaired", "log-recovered" or
    "refreshed"; None if it did not."""
    for key, outcome in (("repaired_by_run", "repaired"), ("log_recovered_by_run", "log-recovered"),
                         ("refreshed_by_run", "refreshed")):
        if meta.get(key) == run_id:
            return outcome
    return None


def bytes_taken(outcome, meta, run_id):
    """The bytes run_id downloaded of a build, as its outcome and meta.json say: all its files if it archived
    it, only those it took again if it updated it (see updated_by), none otherwise."""
    if outcome in ("archived", "archived-without-log", "backfilled"):
        return sum(meta[k]["bytes"] for k in ("log", "podinfo") if meta.get(k))
    if outcome not in ("log-recovered", "refreshed", "repaired"):
        return 0
    again = set(meta.get("refreshed") or ()) if meta.get("refreshed_by_run") == run_id else set()
    again |= {"build-log.txt"} if outcome == "log-recovered" else set()
    again |= set(meta.get("repaired") or ()) if outcome == "repaired" else set()
    return sum(meta[k]["bytes"] for k, name in (("log", "build-log.txt"), ("podinfo", "podinfo.json"))
               if meta.get(k) and name in again)


def add_sightings(target, rec, run_id):
    """For a build whose worker the run gave up on, but whose copy is intact: merge rec's red
    tab sightings into its meta.json (so one from a gap search is kept), and say what this
    run did to it, as its meta.json records."""
    try:
        meta = read_json(os.path.join(target, "meta.json"))
        tabs, changed = merge_tabs(meta["tabs"], [t for t in rec["tabs"] if t.get("red_cells") or cells_left_out(t)])
        if changed:
            write_json(os.path.join(target, "meta.json"), dict(meta, tabs=tabs))
    except (ValueError, OSError, KeyError, TypeError, AttributeError):
        return "already-archived"  # (an unreadable meta.json is reported by the index)
    if meta.get("archived_by_run") == run_id:
        new = "backfilled" if meta.get("backfilled") else "archived" if meta.get("log") else "archived-without-log"
        return "repaired" if meta.get("repaired_by_run") == run_id else new
    return updated_by(meta, run_id) or "already-archived"


def due_in_gcs(target, sampled):
    """Whether an archived build is due to be compared with GCS (see recheck): until it is
    final, or when it is in the run's sevenths (sampled)."""
    try:
        meta = read_json(os.path.join(target, "meta.json"))
    except (ValueError, OSError):
        return False  # (reported as unreadable by the index)
    return isinstance(meta, dict) and (not meta.get("final") or sampled)


def recheck_rec(meta, now, key):
    """The build record of an archived meta.json, so it can be re-checked after TestGrid drops
    it; None unless it is an archive record of the build `key` ("job/build") names."""
    if not valid_meta(meta) or f"{meta.get('job')}/{meta.get('build')}" != key:
        return None
    rec = {k: meta.get(k) for k in ("job", "build", "query", "started", "created", "tabs")}
    if meta.get("backfilled"):
        rec["backfilled"] = meta["backfilled"]
    return rec if valid_rec(rec, now) else None


def job_margin(entry, fresh, now, default):
    """(hours to allow this run, what to remember) for one tab's job duration.

    Every duration that may still bind a running build counts: the current one
    (fresh from prowjob.json, else the last known), and an earlier one until it
    has had its own length to run out since it was last in effect. A shorter
    timeout therefore takes over only once builds started under the longer one
    must have finished.
    """
    def hours(v):
        return v if isinstance(v, (int, float)) and not isinstance(v, bool) and 0 < v < 7 * 24 else None
    until = {}
    if isinstance(entry.get("job_hours"), dict):
        for h, u in entry["job_hours"].items():
            with contextlib.suppress(ValueError):
                if hours(float(h)) and plausible_time(u, now + 7 * 86400):
                    until[float(h)] = u
    previous = hours(entry.get("current_job_hours"))
    current = hours(fresh) or previous
    for h in {current, previous} - {None}:  # both may be in effect up to now
        until[h] = max(until.get(h, 0), now + h * 3600)
    binding = {h: u for h, u in until.items() if u > now}
    remember = {"current_job_hours": current} if current else {}
    if binding:
        remember["job_hours"] = {f"{h:g}": u for h, u in sorted(binding.items())}
    return max([default, *binding]), remember


def backfill_gaps(gaps, gcs, pool, deadline, visible, skip, known=frozenset(), tab_rules=None, watched=None,
                  timelines=None):
    """For each tab with a history gap (gaps: [(tab key, its report, the gap's start
    up to the oldest build it shows now, or None; an older stretch (from, to) searched
    with it, or None; and [(from, to, since)]: builds a scan, at `since`, saw that the
    tab no longer shows, after its config changed: judged by what it may have shown
    since)]), the builds GCS has from that time whose
    column TestGrid painted red on that tab but no longer shows: {tab key: [build
    records] or the Exception that stopped it}, plus "watch": builds there not
    decided yet (still running, or unreadable now), to judge once they are, and
    "sightings": {"job/build": [tab sightings]} for builds already archived or known
    to be red (in known, so not in skip) whose column was red on that tab too. Each
    build is read from GCS once, however many tabs' gaps hold it, and judged per tab
    (the rows a tab shows decide; see verdict). A build watched since an earlier gap
    (in `watched`, {"job/build": its record}) is not read here: the tab is added to
    its record, and it is judged on all its tabs once it is decided (see archive())."""
    tab_rules, watched = tab_rules or {}, watched or {}
    out, candidates, since_of = {}, [], {}  # (since_of: per (tab, build) of a changed range only, its `since`)
    ranges = {key: [(lo, hi, None) for lo, hi in ([(floor, r["oldest"])] if floor is not None else []) +
                    ([older] if older else [])] + list(changed or ())
              for key, r, floor, older, changed in gaps}
    listings = {key: [pool.submit(list_build_ids, gcs, r["query"], lo - BACKFILL_SLACK_HOURS * 3600, hi)
                      for lo, hi, _ in ranges[key]]
                for key, r, _, _, _ in gaps}
    cf.wait([f for futs in listings.values() for f in futs], timeout=max(0, deadline - time.monotonic()))
    for key, r, _, _, _ in gaps:
        futs = listings[key]
        if not all(f.done() for f in futs):
            out[key] = TimeoutError("run timeout reached")
            continue
        failed = next((f.exception() for f in futs if f.exception()), None)
        if failed:
            out[key] = failed
            continue
        out[key] = []
        job = r["query"].rsplit("/", 1)[-1]
        found = {}  # build -> the `since` it is judged from (None: any time)
        for (_, _, since), f in zip(ranges[key], futs, strict=True):
            for b in f.result():
                found[b] = None if since is None or b in found and found[b] is None else \
                    min(since, found.get(b) or since)
        for b in sorted(found, key=int):
            name = f"{job}/{b}"
            if b in visible.get(r["query"], ()):
                continue
            if found[b] is not None:
                since_of[(key, b)] = found[b]
            if name in watched:
                tabs = watched[name]["tabs"]
                had = next((t for t in tabs if (t["dashboard"], t["tab"]) == (r["dashboard"], r["tab"])), None)
                if had is None:
                    tabs.append({"dashboard": r["dashboard"], "tab": r["tab"], "tab_status": r.get("status"),
                                 "red_cells": [], "judged_from_gcs": True,
                                 **({"since": found[b]} if found[b] is not None else {})})
                elif found[b] is None:
                    had.pop("since", None)
                elif "since" in had:  # (judged from the earliest scan that saw it, by the configs since)
                    had["since"] = min(had["since"], found[b]) if isinstance(had["since"], (int, float)) else found[b]
            elif name not in skip or name in known:
                candidates.append((key, r, b))
    days, keys = {}, {}
    for key, r, b in candidates:
        days[(r["query"], b)] = max(days.get((r["query"], b), 0), build_days(tab_rules.get(key, {}), created(b)))
        keys.setdefault((r["query"], b), []).append(key)
    timelines = timelines or Timelines(time.time())
    timelines.prime(pool, gcs, days, deadline, events={qb: tab_events(tab_rules, keys[qb]) for qb in days}, days=days)
    columns = {qb: Columns(gcs, qb[0], qb[1], d, None, timelines, tab_events(tab_rules, keys[qb]))
               for qb, d in days.items()}
    looked_up = {qb: pool.submit(cols) for qb, cols in columns.items()}  # (each read, by the default options)
    cf.wait(looked_up.values(), timeout=max(0, deadline - time.monotonic()))
    views = {}  # per build, how each tab whose gap holds it shows its column
    for key, r, b in candidates:
        if isinstance(out[key], Exception):
            continue
        fut = looked_up[(r["query"], b)]
        if not fut.done():
            out[key] = TimeoutError("run timeout reached")
            continue
        job = r["query"].rsplit("/", 1)[-1]
        if fut.exception() is None:
            state, cells = tab_verdict(columns[(r["query"], b)], job, tab_rules.get(key, {}), created(b),
                                       since=since_of.get((key, b)))
            partial = fut.result().partial
        else:
            state, cells, partial = "undecided", [], "unreadable"
        views.setdefault(f"{job}/{b}", []).append((key, r, b, state, cells, partial))
    recs, watch, sightings = {}, {}, {}
    for name, seen in views.items():
        seen = [v for v in seen if not isinstance(out[v[0]], Exception)]  # (a tab whose search failed since)
        if not seen:
            continue
        query, b = seen[0][1]["query"], seen[0][2]
        job = query.rsplit("/", 1)[-1]
        tabs = [{"dashboard": r["dashboard"], "tab": r["tab"], "tab_status": r.get("status"),
                 "red_cells": cells if state == "red" else [], "judged_from_gcs": True,
                 **({"since": since_of[(key, b)]} if (key, b) in since_of else {})}
                for key, r, _, state, cells, _ in seen]
        red = [{k: v for k, v in t.items() if k != "since"} for t, (*_, state, _, _) in zip(tabs, seen, strict=True)
               if state == "red"]
        if name in known:
            if red:
                sightings[name] = red
        elif red:  # red on a tab that shows it red, whatever the other tabs show
            rec = recs[name] = {"job": job, "build": b, "query": query, "started": None, "created": created(b),
                                "tabs": red, "backfilled": backfill_note(red, seen[0][5])}
            # listed under each tab whose gap holds it, so it is kept if another tab's search fails
            # (run() takes each build once)
            for key, _, _, state, _, _ in seen:
                if state == "red":
                    out[key].append(rec)
        elif any(state == "undecided" for *_, state, _, _ in seen):
            rec = watch[name] = {"job": job, "build": b, "query": query, "started": None, "created": created(b),
                                 "tabs": tabs, "watch": True}
            if any(partial for *_, partial in seen):
                # unreadable now: looked at again in this run, without holding back the rest of the gap
                rec["unreadable"] = True
    out["watch"] = list(watch.values())
    out["sightings"] = sightings
    return out


def main(argv=None):
    cpus = cpu_count()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--archive", required=True, help="archive root directory")
    p.add_argument("--dashboard", action="append", dest="dashboards", type=tgc.dashboard_name,
                   help=f"default: {' '.join(DASHBOARDS)}")
    p.add_argument("--since-hours", type=positive(float),
                   help="only archive builds started within this many hours (default: everything TestGrid shows)")
    p.add_argument("--max-job-hours", type=positive(float), default=6,
                   help="longest expected job duration, used to detect gaps in TestGrid history (default: 6)")
    p.add_argument("--max-concurrency", type=positive(int), default=min(64, 16 * cpus),
                   help="ceiling for in-flight requests (default: 16 per CPU, at most 64)")
    p.add_argument("--start-concurrency", type=positive(int), default=4 * cpus,
                   help="in-flight requests to start with before adapting (default: 4 per CPU)")
    p.add_argument("--run-timeout-minutes", type=positive(float), default=45,
                   help="stop waiting for downloads after this long; unfinished builds are retried next run")
    p.add_argument("--gzip", action="store_true",
                   help="store logs as build-log.txt.gz (meta.json keeps the raw log's md5 and size)")
    p.add_argument("--max-file-mb", type=positive(float), default=95,
                   help="store a file larger than this (MiB) as numbered parts of this size, as GitHub rejects "
                        "files over 100 MB (default: 95)")
    p.add_argument("--testgrid-config", metavar="PATH",
                   help="TestGrid's config for the tabs, as testgrid_config.sh writes it: the rows each tab shows and "
                        "its test group's options, to judge builds TestGrid no longer shows (default, or when it "
                        "cannot be read: the config the last run read, kept in state.json)")
    only = p.add_mutually_exclusive_group()  # (each does one thing, and no run)
    only.add_argument("--list-only", action="store_true", help="print the red builds without downloading")
    only.add_argument("--show-verifications", action="store_true",
                      help="only put each run's verification, as its verification.json records it, in INDEX.md's "
                           "table of runs (the workflow does this once the run is verified)")
    only.add_argument("--clean-partial", action="store_true",
                      help="only remove what cut-off downloads and killed runs left (temp files, build folders without "
                           "meta.json, stored files their meta.json does not list), as a run does at its start (the "
                           "workflow does this before it commits, for a fetch killed before its end)")
    p.add_argument("--testgrid", default=TESTGRID)
    p.add_argument("--gcs", default=GCS)
    args = p.parse_args(argv)

    run_start = time.time()
    tool_version()  # (asked once, now: the run's report gives it)
    root = args.archive
    # (a run makes the archive where nothing is; --show-verifications and --clean-partial have nothing to change)
    # ("x/" would make lexists follow a link, and deny a file)
    if not os.path.isdir(root) and (os.path.lexists(root.rstrip(os.sep) or os.sep) or args.show_verifications
                                    or args.clean_partial):
        print(f"ERROR {root} is not a folder: no archive is there", file=sys.stderr)
        return 1
    if args.show_verifications or args.clean_partial:
        lock = lock_archive(root)
        if lock is None:
            print(f"ERROR another run holds {os.path.join(root, '.lock')}", file=sys.stderr)
            return 2
        with lock:
            if args.show_verifications:
                n = show_verifications(root)
                print(f"INDEX.md: {n} " + ("row of runs shows its" if n == 1 else "rows of runs show their")
                      + " verification anew")
            if args.clean_partial:
                sweep_parts(root)
                warnings = []
                clean_partial_builds(root, warnings)
                for w in warnings:
                    print("WARNING", w, file=sys.stderr)
        return 0
    lock = None
    if not args.list_only:
        lock = lock_archive(root)
        if lock is None:
            print(f"ERROR another run holds {os.path.join(root, '.lock')}", file=sys.stderr)
            return 2
    deadline = time.monotonic() + args.run_timeout_minutes * 60
    ctx = RunContext(AdaptiveLimit(args.start_concurrency, args.max_concurrency), deadline)
    global _last_run
    _last_run = ctx
    caller = context()  # (bound again when the run returns: a run stopped part-way leaves its own abandoned)
    bind_context(ctx)
    # keep a kept-alive connection for every request that can be in flight (pools made from now on)
    HTTP.connection_pool_kw["maxsize"] = max(HTTP.connection_pool_kw["maxsize"], ctx.limit.ceiling)
    pool = cf.ThreadPoolExecutor(ctx.limit.ceiling, initializer=bind_context, initargs=(ctx,))  # (shared by the origins)
    # The GitHub runner ends a timed-out or cancelled step with SIGINT, then SIGTERM;
    # handle both the same way (SIGINT too where it was ignored when Python started, as in a
    # shell's background job).
    previous_term = signal.signal(signal.SIGTERM, interrupt)
    previous_int = signal.signal(signal.SIGINT, interrupt)
    try:
        return run(args, root, run_start, pool, deadline)
    except KeyboardInterrupt:  # outside the download wait (see run()): nothing to record yet
        with contextlib.suppress(KeyboardInterrupt):  # (another signal, while it waits for the workers)
            ctx.abandon()  # workers stop at their next read and cannot write
        print("ERROR interrupted; unfinished builds are picked up by the next run", file=sys.stderr)
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        bind_context(caller)
        # Workers still blocked past the deadline are abandoned: they cannot write
        # any more (see replace()), and the process exit (os._exit, below) does not
        # wait for them.
        pool.shutdown(wait=not ctx.abandoned.is_set(), cancel_futures=True)
        if lock:
            lock.close()


def tab_error_words(r):
    """How the log names a tab's report with an error: "ERROR tab <dashboard> <tab>", or, without
    a tab (a dashboard's summary that could not be read, or a tab name in it that cannot be used),
    "ERROR dashboard <dashboard>"."""
    return ("ERROR dashboard", r["dashboard"]) if r["tab"] is None else ("ERROR tab", r["dashboard"], r["tab"])


def interrupt(signum, frame):
    raise KeyboardInterrupt


def run(args, root, run_start, pool, deadline):
    ctx = context()
    warnings = []
    # (--list-only fetches and writes nothing: it goes on, and warns)
    try:
        excluded = load_excluded(root)
    except ExcludedUnreadable as e:
        if not args.list_only:
            print(f"ERROR {e}: no run goes on until it is fixed, as it could fetch a build taken out", file=sys.stderr)
            return 1
        excluded = frozenset()
        warnings.append(f"{e}: the builds it takes out are listed too")
    # (a link in the archive is never followed: the run's folder would be made where it leads)
    month = dt.datetime.fromtimestamp(run_start, dt.UTC).strftime("%Y-%m")
    for path in (os.path.join(root, "runs"), os.path.join(root, "runs", month)):
        if os.path.islink(path) and not args.list_only:
            print(f"ERROR {path} is a link: no run goes on until it is fixed, as it would write where it leads",
                  file=sys.stderr)
            return 1
        if os.path.islink(path):
            warnings.append(f"{path} is a link: the run folders in it are not read")
        elif os.path.lexists(path) and not os.path.isdir(path) and not args.list_only:  # (the run's folder goes in it)
            print(f"ERROR {path} is not a folder: no run goes on until it is fixed", file=sys.stderr)
            return 1
    timelines = Timelines(run_start)  # (each job's builds that TestGrid's reads of a judged build depend on)
    if not args.list_only:
        sweep_parts(root)
        migrate_old_layout(root, warnings)
        clean_partial_builds(root, warnings)
    state = load_state(root, warnings)
    for name in ("unresolved", "given_up", "watch"):  # (a build taken out is not retried or watched)
        state[name] = {k: v for k, v in state[name].items() if k not in excluded}
    known, duplicates = archived_builds(root)
    # builds taken out for good (excluded.json): their folders go, and they are never fetched again
    taken_out, take_out_errors = sorted(k for k in excluded if k in known), []
    for key in taken_out:
        rel = known.pop(key)
        while rel is not None and not args.list_only:  # (each copy: two run folders can hold one build)
            try:
                remove_build(os.path.join(root, rel))
                # (the last build of the folder of a run cut off before its report, left by code before the
                # README from the start: the folder stays, as such a run's does, see gcs_sevenths)
                folder = rel.rsplit("/", 2)[0]
                if not os.path.lexists(os.path.join(root, folder, "run.json")) and not listdir(os.path.join(root, folder)):
                    write_cut_off_readme(root, folder)
            except OSError as e:
                take_out_errors.append({"build": key, "error": f"taking it out ({EXCLUDED}) failed: "
                                                              f"{type(e).__name__}: {e}"})
                break
            rel = archived_builds(root)[0].get(key)
    if taken_out and not args.list_only:
        duplicates = archived_builds(root)[1]  # (as left: a copy taken out is no longer one of two)
    # the archived builds in this run's sevenths: compared with GCS even if final (cross_check.py
    # compares them too), see gcs_sevenths
    sevenths = gcs_sevenths(root, run_start)
    resampled = {key for key in known if sampled(key, sevenths)}
    folders = run_dirs(root)
    archive_start = folder_time(folders[0].rsplit("/", 1)[1]) if folders else None
    dashboards = args.dashboards or DASHBOARDS
    # TestGrid's config over time (see ConfigHistory): the upload job's runs and their configs
    # read now, with those earlier runs read. Each tab's config now is that of the latest run
    # whose config is known ("read" now when no later run's is unknown), else the one an
    # earlier run of the archive read (each tab's with when it was read).
    history = ConfigHistory(state.get("config_uploads"), state.get("config_entries"), warnings,
                            state.get("config_relist_after"), run_start)
    loaded = read_config(args.testgrid_config, warnings) if args.testgrid_config else None
    if loaded:
        history.merge(loaded, run_start)
    history.prune(run_start)
    current, current_run = history.current()
    unknown_runs = history.unknown()
    later_unknown = [r for r in unknown_runs if current_run is None or int(r["build"]) > int(current_run["build"])]
    config = current if loaded and current is not None and not later_unknown else None  # (the config read now)
    config_source = None
    if config is not None:  # (built by this run or an earlier one)
        config_source = dict(test_infra=current_run["commit"], **(current_run["built"] or {}),
                             upload=current_run["build"], uploaded=current_run["finished"])
    tab_config = {k: c["config"] for k, c in state["tab_configs"].items() if "config" in c}
    config_read = {k: c.get("read") for k, c in state["tab_configs"].items() if "config" in c}
    tab_config.update(current or {})
    config_read.update(dict.fromkeys(config or (), run_start))
    rules = {key: tgc.TabRule(entry) for key, entry in tab_config.items()}

    def check(dashboard, tab, table, query):
        rule = rules.get(f"{dashboard}#{tab}")
        return config_mismatch(rule, table, query) if rule else None
    reports, builds, visible, running, shown_on = scan(args.testgrid, args.gcs, dashboards, pool, deadline, check)
    shown_out = {f"{job}/{build}" for job, build in builds if f"{job}/{build}" in excluded}
    builds = {k: b for k, b in builds.items() if f"{k[0]}/{k[1]}" not in excluded}
    running = {k: b for k, b in running.items() if f"{k[0]}/{k[1]}" not in excluded}
    timings = {"scan": round(time.time() - run_start, 2)}
    job_hours = {}  # per tab, the job durations to remember for the next run
    for r in reports:
        # A job known to run longer than --max-job-hours gets its own margin (see job_margin).
        if r.get("tab") is not None and not r.get("error"):
            key = f"{r['dashboard']}#{r['tab']}"
            r["max_job_hours"], job_hours[key] = job_margin(state["tabs"].get(key) or {}, r.get("max_job_hours"),
                                                            run_start, args.max_job_hours)

    # per tab: the job rows it shows and its days of results, to judge builds it no longer shows
    # (for a tab not read now, as it was when last read)
    tab_rules = {key: {"rows": t.get("rows"), "days": tab_days(t.get("oldest"), t["scan"])}
                 for key, t in state["tabs"].items()}
    tab_rules.update({f"{r['dashboard']}#{r['tab']}": {"rows": r.get("rows"), "days": tab_days(r.get("oldest"), run_start)}
                      for r in reports if r.get("tab") is not None and not r.get("error") and r.get("oldest")})
    # A tab whose table shows a row its config's filters hide (or builds of another GCS
    # folder) does not follow that config (yet): remembered per tab, and a build made before
    # the tab agrees with it again is judged by the rows its table showed too (tab_verdict).
    mismatch_of = {k: c["mismatch"] for k, c in state["tab_configs"].items() if "mismatch" in c}
    for r in reports:
        if r.get("tab") is None or "config_disagrees" not in r:
            continue
        key, found = f"{r['dashboard']}#{r['tab']}", r.pop("config_disagrees")
        prev = mismatch_of.get(key)
        if found:
            mismatch_of[key] = {"detail": found, "since": prev["since"] if prev and prev["until"] is None else run_start,
                                "until": None}
        elif prev and prev["until"] is None:
            mismatch_of[key] = dict(prev, until=run_start)
    mismatch_of = {k: m for k, m in mismatch_of.items()
                   if m["until"] is None or run_start - m["until"] <= (HELD_DAYS + 1) * 86400}
    # ... and where TestGrid's config is known, the rows its base options keep and its days of
    # results; a tab whose test group the port does not follow is not judged
    for key, rule in rules.items():
        m = mismatch_of.get(key)
        tab_rules[key] = dict(tab_rules.get(key, {}), days=rule.days,
                              shows=None if rule.filter_problem or rule.broken or rule.not_updated else rule.shows,
                              problem=rule.group_problem, unreliable_until=(m["until"] or math.inf) if m else -math.inf,
                              options=rule.options)
    # ... and each config the tab had over time ("eras", see tab_verdict), with the updates its
    # test group's config changes made TestGrid read its columns again at ("events")
    era_rules = {}

    def era_rule(entry):
        if entry is None or entry is UNKNOWN:
            return entry
        return era_rules.setdefault(entry_id(entry), tgc.TabRule(entry))
    listed_ok = sorted(f"{r['dashboard']}#{r['tab']}" for r in reports if r.get("tab") is not None and not r.get("error"))
    # the tabs the dashboards' summaries list now (a tab whose table could not be read too), and the
    # dashboards whose summary this run read
    listed = {f"{r['dashboard']}#{r['tab']}" for r in reports if r.get("tab") is not None}
    summaries_ok = {r["dashboard"] for r in reports if r.get("tab") is not None}
    # A tab TestGrid shows now that the latest config known lacks: that is not TestGrid's config
    # (not taken yet, not built yet, or not built for the tab's dashboard), so the tab's config
    # since its last entry is not known. Shown now: listed; on a dashboard this run reads but
    # whose summary it could not read, or that listed only names it cannot use, listed when
    # last read.
    unread = {r["dashboard"] for r in reports} - summaries_ok
    shown = listed | {k for k, t in state["tabs"].items() if k.split("#", 1)[0] in unread and not t.get("missing_since")}
    lacking = sorted(k for k in shown if current is not None and k not in current)
    # (but one a config with the tab may still be in force for, as ConfigHistory.eras takes them, and an
    # hour more, once its run has uploaded it: the latest config may not be TestGrid's yet, and TestGrid
    # then lists the tabs it dropped)
    untaken = {k for k in lacking if history.taken(k, run_start, 3600)}
    # ... and one TestGrid listed so before, past that: its config since its last entry before then is not
    # known either, in the configs that came into force before then, though TestGrid no longer lists it (it
    # showed the tab then), for as long as a build held for it may wait
    lacked = lacking_kept(state.get("lacking_tabs") or {}, run_start)
    lacked.update(dict.fromkeys([k for k in lacking if k not in untaken], run_start))
    queries = {k: t["query"] for k, t in state["tabs"].items() if isinstance(t.get("query"), str)}
    queries.update((f"{r['dashboard']}#{r['tab']}", r["query"]) for r in reports
                   if r.get("tab") is not None and not r.get("error") and r.get("query"))
    for key, got in history_rules(history, sorted({k for r in history.runs if r["tabs"] for k in r["tabs"]} | set(tab_rules)),
                                  run_start, {**lacked, **dict.fromkeys(lacking, run_start)}, queries, era_rule).items():
        tab_rules[key] = dict(tab_rules.get(key, {}), **got)
    previous_read = state.get("testgrid_config") or {}
    source = config_source or previous_read.get("source") or {}
    about = config_about(source) if source else "source unknown"
    # (a run whose config is not known yet, listed less than CONFIG_STALE_DAYS ago: it may be the config
    # of tabs TestGrid shows that the latest known one lacks)
    awaited = [r for r in later_unknown if run_start - first_listed(r) <= CONFIG_STALE_DAYS * 86400]
    for keys, why in ((sorted(untaken), ", which TestGrid may not have taken yet"),
                      ([k for k in lacking if k not in untaken],
                       ", which a later run of its upload job whose config is not known yet may have" if awaited else
                       ": it lacks tabs TestGrid shows (is it built for their dashboard?)")):
        if keys:
            warnings.append(f"TestGrid's config ({about}) has no tab {', '.join(keys[:3])}{', ...' if len(keys) > 3 else ''}"
                            + why + "; builds TestGrid may have shown there since the tab's last config are decided once "
                            "a config with the tab is read")
    if loaded and loaded["failed"]:
        failed = sorted(loaded["failed"].items())
        warnings.append(f"TestGrid's config of {len(failed)} runs of its upload job could not be built now: "
                        + "; ".join(f"{rid}: {why[:300]}" for rid, why in failed[:3]) + ("; ..." if len(failed) > 3 else ""))
    if unknown_runs:
        oldest = unknown_runs[0]
        # (configs are built newest first, as many as a run has time for: a backlog, such as the 18 days of
        # runs a new archive lists, takes runs to build; it is late if one could not be built, or none was,
        # and a build the archive may still judge from GCS could have been shown while it was in force)
        progress = bool(loaded and loaded["configs"])
        since = judged_since(state, run_start)
        late = any(run_start - first_listed(r) > CONFIG_STALE_DAYS * 86400 and (r.get("failed") or not progress)
                   and history.replaced(r["started"] + 1) > since for r in unknown_runs)
        warnings.append(f"TestGrid's config of {len(unknown_runs)} runs of its upload job, from "
                        f"{iso(oldest['started'])} to {iso(unknown_runs[-1]['started'])}, is "
                        + (f"still not known {CONFIG_STALE_DAYS} days after the archive listed it" if late else
                           "not known yet")
                        + ": builds TestGrid may have shown then are decided once a run has read it")
    if loaded and not loaded["runs"]:
        # (the upload job runs several times a day: one that stopped, was renamed or moved lists no run
        # after the last one the archive knows the config of; a run still waiting to be built is listed)
        last = max((r["started"] for r in history.runs), default=None)
        if last is None or run_start - last > CONFIG_STALE_DAYS * 86400:
            warnings.append(f"TestGrid's config upload job ({tgc.UPLOAD_JOB}) has not run since "
                            f"{iso(last) if last is not None else f'{tgc.HISTORY_DAYS} days ago or more'}, as GCS "
                            "lists its runs: changes to TestGrid's config since then are not followed (was it "
                            "renamed, or moved?)")

    def config_warnings(involved):
        """Warn about the config of tabs the run reads or judges builds of."""
        for key in sorted(involved & set(rules)):
            rule, m = rules[key], mismatch_of.get(key)
            if rule.group_problem:
                warnings.append(f"{key}: TestGrid's config for the tab ({about}): {rule.group_problem}; builds the tab "
                                "no longer shows cannot be judged from GCS until this is fixed")
            if rule.filter_problem:
                warnings.append(f"{key}: TestGrid's config for the tab ({about}): {rule.filter_problem}; the filters "
                                "cannot be applied here, so builds the tab no longer shows are judged by the rows its "
                                "table shows")
            if rule.broken:
                warnings.append(f"{key}: TestGrid's config for the tab ({about}): {rule.broken}; TestGrid cannot apply "
                                "its filters either: its tabulator fails the tab, which shows no new column until they "
                                "are fixed")
            if rule.not_updated:
                warnings.append(f"{key}: TestGrid's config for the tab ({about}): {rule.not_updated}: the tab shows "
                                "no new column until this is fixed")
            if rule.unknown_options:
                warnings.append(f"{key}: TestGrid's config for the tab ({about}) sets base options not known here "
                                f"({', '.join(rule.unknown_options)[:200]}): TestGrid's tabulator ignores them, and so "
                                "does the archive; if its page hid rows by them, a build could count as red here that it "
                                "does not show red")
            if m and m["until"] is None:
                warnings.append(f"{key}: TestGrid's config ({about}) does not match the tab since {iso(m['since'])}: "
                                f"{m['detail']}; builds it no longer shows are judged by the rows its table shows too")
        # (not one the latest config known dropped: no config of it is left to read, and its builds are
        # judged by its configs over time)
        stale = [k for k in sorted(involved & set(tab_config)) if (current is None or k in current)
                 and not (plausible_time(config_read.get(k), run_start)
                          and run_start - config_read[k] <= CONFIG_STALE_DAYS * 86400)]
        if stale:
            last = min((config_read[k] for k in stale if plausible_time(config_read.get(k), run_start)), default=None)
            warnings.append(f"TestGrid's config was last read {iso(last) if last else 'at an unknown time'} for "
                            f"{len(stale)} tabs ({', '.join(stale[:3])}{', ...' if len(stale) > 3 else ''}): changes to "
                            "their filters or test groups since then are not followed")
        never = [k for k in listed_ok if k not in tab_config] if args.testgrid_config else []
        if never:
            # (the builds of a tab with configs over time are judged by them, see tab_verdict: those of
            # one the latest config lacks are held until a config with it is read)
            by_rows = any("eras" not in tab_rules.get(k, {}) for k in never)
            warnings.append(f"TestGrid's config was never read for {len(never)} tabs ({', '.join(never[:3])}"
                            f"{', ...' if len(never) > 3 else ''}): "
                            + ("rows their filters hide count as shown" if by_rows else
                               "their builds are decided once a config with them is read")
                            if not awaited else
                            f"TestGrid's config of {len(never)} tabs ({', '.join(never[:3])}"
                            f"{', ...' if len(never) > 3 else ''}) is not known yet: their builds are decided once a "
                            "run has read it")
    read_ok, gaps = {}, []
    # per tab, the ranges this run stops waiting for: (range, whether it is the one derived from the last scan, why)
    dropped, failed_search = {}, set()
    for r in reports:
        if r.get("tab") is None or r.get("error"):
            continue
        key = f"{r['dashboard']}#{r['tab']}"
        prev = state["tabs"].get(key)
        horizon = run_start - GIVE_UP_DAYS * 86400  # a gap is searched this far back in one run
        first = prev is None and archive_start is not None
        pending = (prev or {}).get("unsearched")  # (an older part of a gap, searched a chunk a run)
        if first:
            # never scanned while the archive ran (new, or failing until now): look
            # for red builds since the archive began, as far back as gaps are searched
            prev = {"scan": max(archive_start, horizon), "oldest": None}
        # Builds the last scan saw that might still have been running started after
        # max(that scan's oldest column, its time - max_job_hours); if the oldest
        # column now is later than that, some of them may have scrolled off unseen.
        if prev and r.get("oldest"):
            # (with the margin in effect at that scan, if longer: a build it saw running within it
            # was not overdue then, see stalled)
            margin = max(r["max_job_hours"], prev.get("margin") if isinstance(prev.get("margin"), (int, float))
                         and not isinstance(prev.get("margin"), bool) and 0 < prev["margin"] < 7 * 24 else 0)
            floor = max(prev["oldest"] or -math.inf, prev["scan"] - margin * 3600)
            recent = None
            if r["oldest"] > floor:
                recent = max(floor, horizon) if horizon < r["oldest"] else None
                if floor < horizon and not first:  # (the rest: searched GIVE_UP_DAYS a run, newest first)
                    top = min(horizon, r["oldest"])
                    pending = [min(floor, pending[0]) if pending else floor, max(top, pending[1] if pending else top)]
                    warnings.append(f"{key}: the gap in TestGrid's history reaches back to {iso(floor)}; GCS is "
                                    f"searched {GIVE_UP_DAYS} days of it a run, from {iso(top)} back, over this and "
                                    "the next runs")
            older = None
            if pending:
                older = (max(pending[0], pending[1] - GIVE_UP_DAYS * 86400), pending[1])
            # The builds that scan saw (not red then) that the tab no longer shows: if the tab's
            # config changed since, it may have shown them red before they scrolled off (or before
            # TestGrid dropped them to keep the group's grid under its size limit, as it does the
            # oldest columns of its largest groups): judged by what it may have shown since. Where
            # a config not known yet was in force since, that is decided once it is known (kept
            # in the tab's state until then, GIVE_UP_DAYS at most).
            changed, waiting = [], []
            if not first and prev.get("oldest") is not None:
                kept = list(prev.get("changed_pending") or ())
                seen = list(kept)
                top = min(floor, r["oldest"])
                if prev["oldest"] < top:
                    seen.append({"since": prev["scan"], "lo": prev["oldest"], "hi": top})
                for p in seen:
                    status = config_changed(tab_rules.get(key, {}), p["since"], p["hi"])
                    if status:
                        changed.append((p["lo"], p["hi"], p["since"]))
                    elif status is None and run_start - p["since"] <= GIVE_UP_DAYS * 86400:
                        waiting.append(p)
                    elif status is None:
                        dropped.setdefault(key, []).append((p, p not in kept, "a config of the tab in force since that "
                                                            f"scan is still not known {GIVE_UP_DAYS} days later"))
                for p in waiting[:-PENDING_KEPT]:  # (the oldest past those a tab keeps)
                    dropped.setdefault(key, []).append((p, p not in kept, f"more than {PENDING_KEPT} ranges of the "
                                                        "tab wait for a config"))
                waiting = waiting[-PENDING_KEPT:]
            if recent is not None or older or changed:
                gaps.append((key, r, recent, older, changed))
        if r.get("oldest"):
            read_ok[key] = {"scan": run_start, "oldest": r["oldest"], "margin": r["max_job_hours"], "query": r["query"],
                            **job_hours.get(key, {}), **({"rows": r["rows"]} if known_rows(r.get("rows")) else {})}
            if pending and pending[0] < (older or pending)[0]:  # (what is left once this run searched its chunk)
                read_ok[key]["unsearched"] = [pending[0], older[0]]
            if prev and r.get("oldest") and waiting:
                read_ok[key]["changed_pending"] = waiting
    tab_scans = {}
    for key, prev in sorted(state["tabs"].items()):
        if key.split("#", 1)[0] in summaries_ok and key not in listed:
            if not prev.get("missing_since"):
                warnings.append(f"{key}: no longer listed on its dashboard")
                prev = dict(prev, missing_since=run_start)
            elif run_start - prev["missing_since"] > UNLISTED_DAYS * 86400:
                warnings.append(f"{key}: unlisted for {UNLISTED_DAYS} days; forgetting it")
                # (a tab not listed is not read, so the ranges it kept are never judged)
                warnings += [pending_dropped(key, p, "the tab is no longer listed, and is forgotten")
                             for p in prev.get("changed_pending") or ()]
                if "unsearched" in prev:
                    warnings.append(f"{key}: the older part of its history gap, from {iso(prev['unsearched'][0])} to "
                                    f"{iso(prev['unsearched'][1])}, cannot be searched in GCS: the tab is no longer "
                                    "listed, and is forgotten; red builds in it may be missing")
                continue
        elif key in listed and prev.get("missing_since"):  # (listed again, whether its table could be read or not)
            prev = {k: v for k, v in prev.items() if k != "missing_since"}
        tab_scans[key] = prev
    tab_scans.update(read_ok)

    # A given-up build whose files are back on disk (restored, or archived late) is done;
    # one given up on long ago is no longer listed (its runs' reports still name it).
    given_up = {k: v for k, v in state["given_up"].items()
                if not (k in known and archived_intact(os.path.join(root, known[k]), deep=True))}
    for k, v in list(given_up.items()):
        if run_start - v["first_seen"] > (GIVE_UP_DAYS + GIVEN_UP_KEEP_DAYS) * 86400 \
                and tuple(k.split("/", 1)) not in builds:  # one TestGrid still shows red stays given up
            warnings.append(f"{k}: given up on more than {GIVEN_UP_KEEP_DAYS} days ago; dropped from the index")
            del given_up[k]
    selected = [b for b in builds.values() if f"{b['job']}/{b['build']}" not in given_up]
    if args.since_hours is not None:
        cutoff = run_start - args.since_hours * 3600
        if state["tabs"]:
            # never skip builds that finished since an earlier scan saw them running
            cutoff = min(cutoff, min(t["scan"] - job_margin(t, None, t["scan"], args.max_job_hours)[0] * 3600
                                     for t in state["tabs"].values()))
        selected = [b for b in selected if (b["started"] or b["created"]) >= cutoff
                    or f"{b['job']}/{b['build']}" in state["watch"]]  # (one watched is followed to its end)
    chosen = {f"{b['job']}/{b['build']}" for b in selected}
    retry = [u["rec"] for k, u in state["unresolved"].items() if k not in chosen and k not in given_up]
    # Builds a history gap showed still running, or TestGrid showed running long past their
    # time: judged by TestGrid's rule from GCS, once TestGrid no longer shows them (while it
    # does, what it shows decides, and they stay watched).
    running_on = {}
    for (_, build), rec in running.items():
        for t in rec["tabs"]:
            running_on.setdefault(f"{t['dashboard']}#{t['tab']}", set()).add(build)
    watched = {k: u for k, u in state["watch"].items()
               if k not in chosen and k not in given_up and k not in state["unresolved"] and k not in known}
    # A watched build a tab shows running now stays watched. Otherwise it is judged by the rule,
    # from GCS, on its tabs: what a tab shows now is only its last read, by the tab's config
    # now, and it may have shown another read of it red, or by another config (see
    # tab_verdict). But a tab that shows it now and whose config the port does not follow
    # (which would hold it) is decided by what it shows. (A tab that shows it red now has it
    # chosen.)
    shown_settled, still_shown = [], []
    for k, u in sorted(watched.items()):
        rec = u["rec"]
        showing = {key for key, builds in shown_on.items() if rec["build"] in builds}
        if any(rec["build"] in running_on.get(key, ()) for key in showing):
            still_shown.append(rec)
            continue
        rest = [t for t in rec["tabs"] if not (f"{t['dashboard']}#{t['tab']}" in showing and
                                                (tab_rules.get(f"{t['dashboard']}#{t['tab']}") or {}).get("problem"))]
        if rest:
            retry.append(dict(rec, tabs=rest))
        else:
            shown_settled.append(k)
    # Archived builds that are not final yet are compared with GCS again, even
    # after TestGrid drops them: their logs may still be replaced by Prow. So are
    # the final ones in this run's sevenths (see gcs_sevenths).
    queued = chosen | set(state["unresolved"]) | set(given_up) | set(state["watch"])
    # A column TestGrid still shows as running long after its job should have ended
    # may never finish: TestGrid paints it red 24 hours after it started, and it can
    # scroll off before a later scan sees that, with no gap showing as the history
    # still overlaps. Such builds are watched, and judged from GCS from the next run on.
    margins = {f"{r['dashboard']}#{r['tab']}": r["max_job_hours"] for r in reports if r.get("max_job_hours")}
    stalled = {f"{job}/{build}": rec for (job, build), rec in sorted(running.items())
               if (job, build) not in builds and f"{job}/{build}" not in known and f"{job}/{build}" not in queued
               and run_start - rec["created"] <= GIVE_UP_DAYS * 86400
               and (rec["started"] or rec["created"]) < run_start - 3600 * max(
                   margins.get(f"{t['dashboard']}#{t['tab']}", args.max_job_hours) for t in rec["tabs"])}
    rechecks = 0
    for key, rel in sorted(known.items()):
        if key in queued:
            continue
        try:
            meta = read_json(os.path.join(root, rel, "meta.json"))
        except (ValueError, OSError):
            continue  # reported as unreadable by the index
        if valid_meta(meta) and (not meta.get("final") or key in resampled) and (rec := recheck_rec(meta, run_start, key)):
            retry.append(rec)
            rechecks += 1
    # A gap in a tab's history: list the job's builds from that time in GCS and
    # archive the ones TestGrid painted red by its own rule, since it no longer shows them.
    backfill_errors, backfilled, new_watch, taken, running_gap, searched = [], 0, {}, set(), [], set()

    def last_scan(key):
        return (state["tabs"].get(key) or {"scan": archive_start})["scan"]
    if gaps and not args.list_only:
        # Builds retried anyway are not taken again: one known to be red (waiting to be
        # archived) is only judged for the gap's tab, and one watched gains that tab.
        in_retry = {f"{x['job']}/{x['build']}": x for x in retry}
        backfill = backfill_gaps(gaps, args.gcs, pool, deadline, visible, set(known) | queued | excluded,
                                 known=(set(known) - queued) | (in_retry.keys() & set(state["unresolved"])),
                                 tab_rules=tab_rules, watched={k: x for k, x in in_retry.items() if x.get("watch")},
                                 timelines=timelines)
        new_watch = {f"{w['job']}/{w['build']}": w for w in backfill.pop("watch", [])}
        # an unreadable one is looked at again now; one still running is watched until it is not
        for w in new_watch.values():
            (retry if w.pop("unreadable", False) else running_gap).append(w)
        # Archived (or known red) builds whose column was red on a tab whose gap holds them
        # too: that tab is added to their record (recheck() and store() take it from there).
        for name, tabs in sorted(backfill.pop("sightings", {}).items()):
            rec = next((x for x in retry if f"{x['job']}/{x['build']}" == name), None)
            if rec is None:
                try:
                    rec = recheck_rec(read_json(os.path.join(root, known[name], "meta.json")), run_start, name)
                except (ValueError, OSError):
                    rec = None
                if rec is None:
                    continue  # reported as unreadable by the index
                rec["tabs"] = []
                retry.append(rec)
                rechecks += 1
            had = {(t["dashboard"], t["tab"]) for t in rec["tabs"]}
            rec["tabs"] = rec["tabs"] + [t for t in tabs if (t["dashboard"], t["tab"]) not in had]
        for key, r, floor, older, changed in gaps:
            found = backfill.get(key)
            what = []  # (what the gap's search covered: the recent part, an older one, those seen before a change)
            if floor is not None:
                what.append(f"TestGrid only shows builds since {iso(r['oldest'])}, but the last scan "
                            f"({iso(last_scan(key))}) needed them from {iso(floor)}")
            if older:
                what.append(f"the older part of its history gap from {iso(older[0])} to {iso(older[1])} is searched "
                            "now" + (f" (the rest, back to {iso(read_ok[key]['unsearched'][0])}, by the next runs)"
                                     if "unsearched" in read_ok.get(key, {}) else ""))
            for lo, hi, since in changed or ():
                what.append(f"its config changed since a scan ({iso(since)}) saw the builds from {iso(lo)} to "
                            f"{iso(hi)}, which it no longer shows")
            if isinstance(found, Exception):
                backfill_errors.append({"build": None, "error": f"{key}: listing GCS for the history gap failed: "
                                                               f"{type(found).__name__}: {found}"})
                # Keep the old scan time (none for a tab not scanned before), and what was left of an
                # older gap, so the next run searches them again.
                if key in state["tabs"]:
                    old = state["tabs"][key]
                    # (but not a range this run stopped waiting for: it is dropped, and said so below)
                    gone = [p for p, derived, _ in dropped.get(key, ()) if not derived]
                    pending = [p for p in old.get("changed_pending", ()) if p not in gone]
                    tab_scans[key] = dict(job_hours.get(key, {}), scan=old["scan"], oldest=old["oldest"],
                                          query=r["query"], **({"margin": old["margin"]} if "margin" in old else {}),
                                          **({"rows": r["rows"]} if known_rows(r.get("rows")) else {}),
                                          **({"unsearched": old["unsearched"]} if "unsearched" in old else {}),
                                          **({"changed_pending": pending} if pending else {}))
                else:
                    tab_scans.pop(key, None)
                failed_search.add(key)
                warnings.append(f"{key}: {'; '.join(what)}; red builds in between may be missing; looking them up "
                                "in GCS failed; the next run tries again")
                continue
            searched.add(key)
            new = [rec for rec in found if f"{rec['job']}/{rec['build']}" not in taken]  # also in another tab's gap
            taken.update(f"{rec['job']}/{rec['build']}" for rec in new)
            retry += new
            backfilled += len(new)
            undecided = [w for w in new_watch.values() if any(f"{t['dashboard']}#{t['tab']}" == key for t in w["tabs"])]
            held = sum(1 for w in undecided if held_by(w, tab_rules))
            why = [f"{held} held until the config TestGrid had then is known and followed here"] * bool(held) + \
                [f"{len(undecided) - held} still running, or unreadable"] * bool(len(undecided) - held)
            warnings.append(f"{key}: {'; '.join(what)}; GCS was searched instead, and {len(found)} red builds from "
                            "that time were taken from it"
                            + (f" ({len(undecided)} not decided yet, looked at again: {'; '.join(why)})"
                               if undecided else ""))
    elif gaps:
        warnings += [f"{key}: TestGrid only shows builds since {iso(r['oldest'])}, but the last scan "
                     f"({iso(last_scan(key))}) needed them from "
                     f"{iso(floor if floor is not None else older[0] if older else min(c[0] for c in changed))}"
                     "; red builds in between may be missing" for key, r, floor, older, changed in gaps]
    # Each range this run stopped waiting for, but the one derived from a scan that a failed gap search
    # keeps: the next run derives it again.
    warnings += [pending_dropped(key, p, why) for key, ranges in sorted(dropped.items()) for p, derived, why in ranges
                 if not (derived and key in failed_search)]
    tab_errors = [r for r in reports if r.get("error")]
    # tabs whose builds this run judges from GCS (a history gap, or watched builds) without any config
    # of theirs: neither one read nor their configs over time (by which tab_verdict judges them otherwise)
    judged = searched | {f"{t['dashboard']}#{t['tab']}" for x in retry + running_gap + list(stalled.values()) + still_shown
                         if x.get("watch") for t in x["tabs"]}
    involved = set(listed_ok) | judged
    without = sorted(k for k in judged - set(rules) if "eras" not in tab_rules.get(k, {}))
    if without:
        warnings.append(f"no TestGrid config known for {len(without)} tabs ({', '.join(without[:3])}"
                        f"{', ...' if len(without) > 3 else ''}): for the builds they no longer show, judged from GCS "
                        "now, rows their base options may hide count as shown, and their days of results are taken "
                        "from their history, or are TestGrid's default for a tab the archive no longer keeps")

    if args.list_only:
        config_warnings(involved)
        for b in sorted(selected + retry, key=lambda b: b["started"] or b["created"]):
            print(iso(b["started"] or b["created"]), b["job"], b["build"], json.dumps([t["tab"] for t in b["tabs"]]))
        print(f"{tabs_read_text(reports)}, {len(selected)} red builds, "
              f"{len(retry) - rechecks} to retry, {rechecks} archived builds not shown red now to compare with GCS again")
        for r in tab_errors:
            print(*tab_error_words(r), r["error"], file=sys.stderr)
        for w in warnings:
            print("WARNING", w, file=sys.stderr)
        return 1 if tab_errors else 0

    run_id, run_rel = new_run_dir(root, run_start)
    for r in reports:  # (what cross_check judges the run's builds by)
        key = f"{r['dashboard']}#{r['tab']}" if r.get("tab") is not None else None
        if key in tab_config:
            r.update(config=tab_config[key], config_read=config_read.get(key))
        if key in mismatch_of:
            r["config_mismatch"] = mismatch_of[key]
    # A README from the start, that the run was cut off, as one killed before it ends leaves it (a step's
    # timeout kills it at last): its folder stays, though what it fetches again is stored in older ones,
    # and later runs see a run killed before its first report as not done with its sevenths (see
    # sevenths_done). The run writes it again once it has its report; each later run does for a run that
    # did not end (see write_index).
    write_cut_off_readme(root, run_rel)
    outcomes, errors, downloaded, unresolved, watch = {}, list(backfill_errors) + take_out_errors, 0, {}, {}
    uncompared = []  # (archived builds due to be compared with GCS whose comparison did not end)
    still_failing = {}  # (archived builds given up on whose comparison failed again: why, see NotCompared)
    if taken_out or shown_out:  # (taken out of the archive now, or shown red by TestGrid and left out)
        outcomes["excluded"] = sorted(set(taken_out) | shown_out)
    recheck_failed = False  # (a comparison with GCS failed: of a build not given up on in this run or before, or
    # not on GCS's side)
    # judged by the gap search just now and still running, overdue on TestGrid, or watched and still
    # shown by TestGrid: watched without a look now, for as long as a build is ever watched
    for w in running_gap + list(stalled.values()) + still_shown:
        key = f"{w['job']}/{w['build']}"
        held = held_by(w, tab_rules)
        if run_start - w["created"] > (HELD_DAYS if held else GIVE_UP_DAYS) * 86400:
            outcomes.setdefault("stopped-holding" if held else "stopped-watching", []).append(key)
            continue
        outcomes.setdefault("held" if held else "watching", []).append(key)
        watch[key] = {"rec": w, "first_seen": state["watch"].get(key, {}).get("first_seen", run_start)}
    if shown_settled:
        outcomes.setdefault("not-red", []).extend(shown_settled)
    errors += [{"build": None, "error": f"duplicate copies: {d}"} for d in duplicates]
    max_bytes = max(1, int(args.max_file_mb * (1 << 20)))

    def target_of(b):
        rel = known.get(f"{b['job']}/{b['build']}") or f"{run_rel}/{b['job']}/{b['build']}"
        return os.path.join(root, rel)

    hours_of = {f"{r['dashboard']}#{r['tab']}": r["max_job_hours"] for r in reports if r.get("max_job_hours")}

    def hours(b):  # the longest any of the build's tabs says its job can run (as last known, for a tab not read now)
        keys = [f"{t['dashboard']}#{t['tab']}" for t in b["tabs"]]
        return max([args.max_job_hours] + [hours_of.get(k) or job_margin(state["tabs"].get(k) or {}, None, run_start,
                                                                         args.max_job_hours)[0] for k in keys])
    if ctx.abandoned.is_set():
        # The run timed out while scanning: nothing can be fetched now. Archived
        # builds stay as they are; the rest are queued for the next run below.
        futures = {}
        for b in selected + retry:
            key = f"{b['job']}/{b['build']}"
            if key in known:
                outcomes.setdefault("not-checked", []).append(key)
                if due_in_gcs(os.path.join(root, known[key]), key in resampled):
                    uncompared.append(key)
            elif b.get("watch"):
                watch[key] = {"rec": b, "first_seen": state["watch"].get(key, {}).get("first_seen", run_start)}
            else:
                previous = state["unresolved"].get(key, {})
                unresolved[key] = {"rec": b, "first_seen": previous.get("first_seen", run_start),
                                   "last_error": "TimeoutError: run timeout reached before downloads started"}
                errors.append({"build": key, "error": "TimeoutError: run timeout reached before downloads started"})
    else:
        watched = {(b["query"], b["build"]): judged_with(b, tab_rules) for b in selected + retry if b.get("watch")}
        timelines.prime(pool, args.gcs, watched, deadline, events={k: v[1] for k, v in watched.items()},
                        days={k: v[0] for k, v in watched.items()})
        futures = {pool.submit(archive, b, target_of(b), args.gcs, run_id, run_start, hours(b), args.gzip,
                               max_bytes, tab_rules, timelines, compare_final=f"{b['job']}/{b['build']}" in resampled): b
                   for b in selected + retry}
    interrupted = False
    try:
        _, not_done = cf.wait(futures, timeout=max(0, deadline - time.monotonic()))
    except KeyboardInterrupt:
        # Interrupted while downloading: stop the workers, but still record what
        # was archived and remember the rest for the next run.
        interrupted = True
        not_done = {f for f in futures if not f.done()}
    if not_done:
        try:
            ctx.abandon()
        except KeyboardInterrupt:  # (one while it waited for a worker to finish putting files in place)
            interrupted = True

    # A build whose comparison with GCS failed after this run took a file of it again (in place, see
    # RecheckError) is listed as updated, as when the comparison does not fail; returned: the bytes it took.
    def taken_before_failing(key, meta):
        updated = updated_by(meta, run_id)
        if updated is None:
            return 0
        outcomes.setdefault(updated, []).append(key)
        return bytes_taken(updated, meta, run_id)
    for fut in futures:
        b = futures[fut]
        key = f"{b['job']}/{b['build']}"
        previous = state["unresolved"].get(key, {})
        try:
            # (a worker the run gave up on that has ended since, but not for being stopped, is taken as one
            # that ended in time: none can write once abandon() returns)
            if fut in not_done and (not fut.done() or isinstance(fut.exception(), Abandoned)):
                if archived_intact(target_of(b), deep=True):
                    # Stopped part-way, or already on disk. One due to be compared with GCS was
                    # not compared to the end, even if this run took a file of it again first
                    # ("not-checked" if it did not update it): the next run compares it (the
                    # run is not done with its sevenths, see gcs_sevenths).
                    outcome = add_sightings(target_of(b), b, run_id)
                    if outcome in ("already-archived", "refreshed", "log-recovered") \
                            and due_in_gcs(target_of(b), key in resampled):
                        uncompared.append(key)
                        if outcome == "already-archived":
                            outcome = "not-checked"
                    outcomes.setdefault(outcome, []).append(key)
                    # (what it took before it was stopped, as its meta.json says; one that cannot be read is
                    # reported by the index)
                    with contextlib.suppress(ValueError, OSError, KeyError, TypeError, AttributeError):
                        downloaded += bytes_taken(outcome, read_json(os.path.join(target_of(b), "meta.json")), run_id)
                    continue
                cause = "interrupted" if interrupted else "run timeout reached"
                raise TimeoutError(f"{cause} before this build was done")
            outcome, meta = fut.result()
            if outcome in ("archived", "archived-without-log") and meta.get("backfilled"):
                outcome = "backfilled"
            outcomes.setdefault(outcome, []).append(key)
            downloaded += bytes_taken(outcome, meta, run_id)
            if outcome == "comparison-given-up":  # (said once, as for a red build given up on: see COMPARISON_FAILING)
                errors.append({"build": key, "error": f"no comparison with GCS has succeeded since "
                                                      f"{iso(meta[COMPARISON_FAILING])}: after {GIVE_UP_DAYS} days of "
                                                      "failures, the archived copy is still compared at least once a "
                                                      "week, but from now on a failure on GCS's side is a warning: "
                                                      f"{meta[COMPARISON_GIVEN_UP]}"})
                downloaded += taken_before_failing(key, meta)
            if outcome == "pending":  # (a watched build found red is queued as red from now on)
                unresolved[key] = {"rec": meta, "first_seen": previous.get("first_seen", run_start), "last_error": None}
            elif outcome in ("watching", "held"):
                watch[key] = {"rec": b, "first_seen": state["watch"].get(key, {}).get("first_seen", run_start)}
        except NotCompared as e:  # (no error, nor serious: see COMPARISON_FAILING)
            outcomes.setdefault("given-up-still-failing", []).append(key)
            still_failing[key] = str(e)
            downloaded += taken_before_failing(key, e.meta)
        except RecheckError as e:
            # The archived copy stays as it is (see RecheckError), and the next run compares it again (the
            # run is not done with its sevenths, see gcs_sevenths).
            errors.append({"build": key, "error": str(e)})
            recheck_failed = True
            downloaded += taken_before_failing(key, e.meta)
        except Exception as e:
            message = f"{type(e).__name__}: {e}"
            if b.get("watch") and not getattr(e, "red_rec", None):
                # not known to be red yet: keep watching rather than list it as missing,
                # as long as a build is ever watched
                limit = HELD_DAYS if held_by(b, tab_rules) else GIVE_UP_DAYS
                if run_start - b["created"] <= limit * 86400:
                    watch[key] = {"rec": b, "first_seen": state["watch"].get(key, {}).get("first_seen", run_start)}
                else:
                    message = f"no longer watched after {limit} days, as it could not be judged: {message}"
                errors.append({"build": key, "error": message})
                continue
            b = getattr(e, "red_rec", None) or b  # a watched build found red is handled like any other
            first = previous.get("first_seen", run_start)
            if run_start - first > GIVE_UP_DAYS * 86400:
                message = f"giving up after {GIVE_UP_DAYS} days: {message}"
                given_up[key] = {"rec": b, "first_seen": first, "last_error": message}
            else:
                unresolved[key] = {"rec": b, "first_seen": first, "last_error": message}
            errors.append({"build": key, "error": message})
    for names in outcomes.values():
        names.sort()
    errors.sort(key=lambda e: e["build"] or "")
    if uncompared:
        uncompared.sort()
        warnings.append(f"{len(uncompared)} archived builds were not compared with GCS before the run "
                        f"{'was interrupted' if interrupted else 'reached its deadline'} "
                        f"({', '.join(uncompared[:3])}{', ...' if len(uncompared) > 3 else ''}): the next run "
                        "compares them")
    recs = {f"{b['job']}/{b['build']}": b for b in selected + retry + running_gap + list(stalled.values()) + still_shown}
    warnings += [f"{key}: still not decided {GIVE_UP_DAYS} days after it was made (not finished, or TestGrid still "
                 "shows it running, or it has no usable start time, so TestGrid never paints it red); no longer watched"
                 for key in outcomes.get("stopped-watching", [])]
    warnings += [f"{key}: cannot be judged from GCS: {held_by(recs[key], tab_rules) or 'its tab config'}; no longer "
                 f"watched {HELD_DAYS} days after it was made, though TestGrid may have shown it red"
                 for key in outcomes.get("stopped-holding", []) if key in recs]
    config_warnings(involved)

    timings["downloads"] = round(time.time() - run_start - sum(timings.values()), 2)  # (with the gap search)
    # A possible gap in TestGrid history, a tab gone from its dashboard or a lost
    # retry list means builds may be missing without an error naming them.
    serious = [w for w in warnings if any(k in w for k in SERIOUS)]
    if still_failing:  # (after serious: it quotes GCS's error, whatever that says)
        failing = sorted(still_failing)
        warnings.append(f"comparisons with GCS still fail for {len(failing)} archived build{'s' if len(failing) > 1 else ''}"
                        " the runs gave up on ("
                        + "; ".join(f"{k}: {still_failing[k][:150]}" for k in failing[:3])
                        + f"{'; ...' if len(failing) > 3 else ''}): each is compared again at least once a week")
    config_record = {"source": config_source, "read": run_start} if config_source else previous_read or None
    # (what later runs still have to search or judge: the older parts of history gaps, and the ranges of
    # builds a scan saw that wait for a config not known yet)
    left = {"gap_parts": {k: t["unsearched"] for k, t in sorted(tab_scans.items()) if "unsearched" in t},
            "config_ranges": {k: {"ranges": len(t["changed_pending"]),
                                  "oldest_scan": min(p["since"] for p in t["changed_pending"])}
                              for k, t in sorted(tab_scans.items()) if t.get("changed_pending")}}
    left["unlisted"] = {k: tab_scans[k]["missing_since"] for k in sorted(set(left["gap_parts"]) | set(left["config_ranges"]))
                        if tab_scans[k].get("missing_since")}  # (when TestGrid stopped listing the tab)
    report_ = {"run": run_id, "dir": run_rel, "started": run_start, "tool": tool_version(), "github": github_run(),
               "since_hours": args.since_hours, "max_job_hours": args.max_job_hours, "testgrid_config": config_record,
               "tabs": reports, "red_builds_seen": len(builds),
               "retried_from_state": len(retry) - rechecks - backfilled - (len(new_watch) - len(running_gap)),
               "rechecked": rechecks, "watched": len(watch),
               "backfilled_from_gcs": len(outcomes.get("backfilled", [])), "outcomes": outcomes,
               "errors": errors, "warnings": warnings, "serious_warnings": serious, "interrupted": interrupted,
               # (whether it compared with GCS all the archived builds it was due to, but for the failures on GCS's
               # side of those the runs gave up on: see gcs_sevenths, COMPARISON_FAILING)
               "gcs_compared": not (uncompared or recheck_failed or interrupted),
               # (why the comparison of each of those failed again, see NotCompared)
               **({"given_up_still_failing": dict(sorted(still_failing.items()))} if still_failing else {}),
               "left_for_later": {k: v for k, v in left.items() if v}}
    run_json = os.path.join(root, run_rel, "run.json")
    # Save what the next run needs before drawing the pages and charts: a failure
    # there must not lose a build that is about to scroll off TestGrid.
    referenced = set(tab_scans) | set(config or ()) | listed | {
        f"{t['dashboard']}#{t['tab']}" for kept in (unresolved, given_up, watch) for u in kept.values() for t in u["rec"]["tabs"]}
    tab_configs = {}
    for key in sorted(referenced & (set(tab_config) | set(mismatch_of))):
        c = {}
        if key in tab_config:  # (its group's digest is the history's: the code before that takes no such field)
            c["config"] = {k: v for k, v in tab_config[key].items() if k != "group_digest"}
            if plausible_time(config_read.get(key), run_start):
                c["read"] = config_read[key]
        if key in mismatch_of:
            c["mismatch"] = mismatch_of[key]
        tab_configs[key] = c
    uploads, entries = history.encode()
    # (the report first: a run killed once state.json no longer holds what it stopped waiting for still
    # warns of it there, and one killed before leaves state.json as it was, for the next run to warn)
    write_json(run_json, dict(report_, errors=errors + [{"build": None, "error": STOPPED}]))
    write_json(os.path.join(root, "state.json"),
               {"tabs": tab_scans, "unresolved": unresolved, "given_up": given_up, "watch": watch,
                "tab_configs": tab_configs, **({"testgrid_config": config_record} if config_record else {}),
                **({"lacking_tabs": lacked} if lacked else {}),
                **({"config_uploads": uploads, "config_entries": entries} if uploads else {}),
                **({"config_relist_after": history.relist_after} if history.relist_after is not None else {})})
    # What failed or cut-off downloads left (a build folder without meta.json, stored files
    # a meta.json does not list, an empty job folder) goes now, so it is not committed;
    # those builds are fetched again. Abandoned workers can no longer write (see abandon()).
    try:
        tidy = []
        clean_partial_builds(root, tidy)
        warnings += [w for w in tidy if w not in warnings]  # (the start of the run may have said so already)
    except OSError as e:
        errors.append({"build": None, "error": f"removing what cut-off downloads left failed: {type(e).__name__}: {e}"})
    missing = ([{"rec": u["rec"], "status": u.get("last_error") or "waiting for GCS upload"} for u in unresolved.values()]
               + [{"rec": u["rec"], "status": f"given up: {u.get('last_error') or 'unknown error'}"}
                  for u in given_up.values()])
    entries = None
    try:
        entries, _ = write_index(root, time.time(), report_, missing)  # adds unreadable files to errors
    except Exception as e:
        errors.append({"build": None, "error": f"writing the index or charts failed: {type(e).__name__}: {e}"})
    elapsed = time.time() - run_start
    timings["index"] = round(elapsed - sum(timings.values()), 2)
    cpu = os.times()
    concurrency = {"cpus": cpu_count(), "start": args.start_concurrency, "ceiling": ctx.limit.ceiling, **ctx.stats()}
    final = dict(report_, errors=errors, finished=time.time(), bytes_downloaded=downloaded, concurrency=concurrency,
                 timings=timings, cpu_seconds=round(cpu.user + cpu.system, 2))
    if entries is None:
        # (the index could not be written: the README from the start, that says the run was cut off, goes,
        # and the next run writes one; before the final report, so no run that ended keeps it)
        with contextlib.suppress(OSError):
            os.remove(os.path.join(root, run_rel, "README.md"))
    write_json(run_json, final)
    if entries is not None:
        write_run_readme(root, run_rel, final, entries)
        # (an older run folder's README links a build taken out no more: write_index rewrites recent ones
        # only, and the run that took it out may have been cut off before this, or a person removed it)
        for rel in sorted(linking(root, excluded) - {run_rel}):
            write_run_readme(root, rel, read_report(root, rel), entries)

    counts = [f"{k}={len(v)}" for k, v in sorted(outcomes.items())] + [f"errors={len(errors)}"]
    print(f"run {run_id}: {tabs_read_text(reports)}, {len(builds)} red builds on TestGrid, "
          f"{len(selected)} selected, {len(retry) - rechecks - backfilled - (len(new_watch) - len(running_gap))} retried, "
          f"{rechecks} archived builds not shown red now compared with GCS again, "
          f"{len(outcomes.get('backfilled', []))} taken from GCS by TestGrid's rule: "
          f"{', '.join(counts)}; archive holds {len(entries) if entries is not None else '?'} builds")
    print(f"  {downloaded / 1e6:.1f} MB in {elapsed:.1f}s ({downloaded / 1e6 / max(elapsed, 1e-9):.1f} MB/s; scan "
          f"{timings['scan']:.1f}s, downloads {timings['downloads']:.1f}s, index {timings['index']:.1f}s); CPU "
          f"{final['cpu_seconds']:.1f}s on {concurrency['cpus']} cores ({final['cpu_seconds'] / max(elapsed, 1e-9):.0%} of one)")
    for origin, c in concurrency["origins"].items():
        print(f"  {origin}: in-flight requests start {concurrency['start']}, peak {c['peak']}, final {c['final']} "
              f"of max {concurrency['ceiling']}, {c['pushbacks']} pushbacks")
    print(f"  folder: {os.path.join(root, run_rel)}")
    for r in tab_errors:
        print(*tab_error_words(r), r["error"], file=sys.stderr)
    for e in errors:  # (one of the whole run names no build)
        print(*(("ERROR build", e["build"]) if e["build"] else ("ERROR",)), e["error"], file=sys.stderr)
    for w in warnings:
        print("WARNING", w, file=sys.stderr)
    if interrupted:
        print("ERROR interrupted; unfinished builds are retried by the next run", file=sys.stderr)
        return 130
    return 1 if tab_errors or errors or serious else 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    sys.stderr.flush()
    if _last_run is not None and _last_run.abandoned.is_set():
        os._exit(code)  # do not wait for workers abandoned at the run timeout
    sys.exit(code)
