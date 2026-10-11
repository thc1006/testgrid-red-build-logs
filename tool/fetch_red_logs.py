#!/usr/bin/env python3
"""Archive the raw Prow build-log.txt behind every red cell on TestGrid dashboards.

A cell counts as red exactly when TestGrid's own UI paints it red (#a00):
FAIL, TIMED_OUT, CATEGORIZED_FAIL and TOOL_FAIL. Tables are read the way the
dashboard page loads them (tab base options applied, no extra row filter), so
the red cells are the ones you see when you open the tab. Each column is one
Prow build: the archive keeps one copy per build and skips builds it already
holds, so every run picks up whatever turned red since the previous run and
the first run backfills everything TestGrid still shows. Builds that could not
be archived yet are remembered in state.json and retried straight from GCS,
even after they scroll off TestGrid.

When a tab's history no longer reaches back to the previous scan (a missed run,
or a tab that keeps little history), its builds from that time are listed in
GCS and judged by TestGrid's own rule (testgrid_rule.py, a port of its updater
checked against TestGrid's code): those it painted red on the tab are archived,
and those it would still read again (not finished) are watched until they can
be judged.

A matching md5 only proves the copy equals what GCS held at that moment, and
Prow can upload a log again (its sidecar uploads once when told to stop and
again when the test exits). So every archived build is compared with GCS on
every run until RECHECK_HOURS after it finished (GIVE_UP_DAYS after it was
archived, for one without a log), and fetched again whenever GCS holds
different content. A copy taken less than SETTLE_HOURS after the
build finished, or before it finished at all, is marked as not settled.

Each run gets its own folder; a build is stored in the folder of the run that
first archived it, and later repairs or refreshes happen in place. Layout
under --archive:
  runs/<YYYY-MM>/<run>/             one run, named by its UTC start time
    run.json                        what the run scanned, archived and failed
    README.md                       the builds it archived, with links
    <job>/<build>/build-log.txt     raw log, length and md5 checked against GCS
                                    (build-log.txt.gz with --gzip; meta.json keeps the raw md5)
    <job>/<build>/podinfo.json      only when the build has no log (pod never ran)
    <job>/<build>/<file>.000, ...   a file larger than --max-file-mb, in numbered parts
    <job>/<build>/meta.json         tabs, red cells, TestGrid messages, result, GCS identity
  charts/*.svg                      trend charts shown in INDEX.md
  INDEX.md                          charts, builds still missing, recent runs and builds
  weeks/<ISO week>.md, .json        every archived build, one page per ISO week (UTC)
  index.json                        the list of weekly pages
  state.json                        per-tab last scan, builds to retry or given up on
"""
import argparse
import base64
import concurrent.futures as cf
import contextlib
import copy
import datetime as dt
import email.utils
import fcntl
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
import signal
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
import testgrid_rule as tr  # noqa: E402

TESTGRID = "https://testgrid.k8s.io"
GCS = "https://storage.googleapis.com"
DASHBOARDS = ["sig-release-master-blocking", "sig-release-master-informing"]
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
LOG_FILES = {"build-log.txt", "build-log.txt.gz", "podinfo.json"}
# a stored file too large for one file in git is kept as numbered parts: <file>.000, <file>.001, ...
PART_RE = re.compile(r"(build-log\.txt(?:\.gz)?|podinfo\.json)\.([0-9]{3})")
RUN_FILES = {"run.json", "README.md"}  # in a run folder, next to its job folders
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
MAX_DOWNLOAD_BYTES = 4 << 30  # a GCS object larger than this is refused rather than streamed forever
MAX_LIST_PAGES = 1000  # a GCS listing that pages on longer than this is broken
GUNZIP_STEP = 256 << 10  # gunzip yields at most this many bytes per step, whatever the ratio
GIVE_UP_DAYS = 14  # stop retrying a build GCS still lacks after this long
# Prow's last upload for a build comes within about 20 minutes of its finished.json
# time, and GCS may keep serving a public object's old content from cache for up to
# 60 minutes after an overwrite (https://cloud.google.com/storage/docs/consistency),
# so only a copy taken or confirmed this long after the build finished is settled.
SETTLE_HOURS = 2
RECHECK_HOURS = 48  # compare an archived build with GCS on every run until this long after it finished
UNLISTED_DAYS = 30  # forget a tab missing from its dashboard's summary after this long
SERIOUS = ("may be missing", "state.json", "no longer listed", "old layout")  # warnings that fail the run
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


def cpu_count():
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


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

    def abandon(self):
        """Stop the workers: once this returns, none is in the middle of changing a file of
        the archive (a rename or a removal) or can start to, as replace() and remove()
        check under the same lock; at most a temp file git ignores can still appear."""
        with self.write_lock:
            self.abandoned.set()

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


def context():
    return getattr(_local, "context", _DEFAULT_CONTEXT)


def bind_context(ctx):
    _local.context = ctx


# Keep-alive connections, one pool per origin, shared by a run's threads: most
# requests are small HEADs and GETs to the same two hosts, and reusing a
# connection saves its TLS handshake (measured: 15-37% faster). request()
# does the retrying, and follows redirects itself (same host only).
HTTP = urllib3.PoolManager(num_pools=16, maxsize=64, block=False, retries=False)
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
        consumed = False
        try:
            if raw.status < 200:
                # an interim response (103 Early Hints) is followed by the real one on the same
                # connection: never read past it, or the next request would get that
                raise IntegrityError(f"{url}: unexpected interim response {raw.status}")
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
        job = get_json(f"{gcs}/{query}/{build}/prowjob.json")
        decoration = job["spec"]["decoration_config"]
        timeout, grace = go_duration(decoration.get("timeout")), go_duration(decoration.get("grace_period") or "0s")
    except Exception:  # missing or unexpected: fall back to --max-job-hours
        return None
    if timeout is None or grace is None or not 0 < timeout + grace < 7 * 86400:
        return None
    return round((timeout + grace) / 3600 + 0.25, 2)


def list_build_ids(gcs, query, lo, hi):
    """Build IDs under gs://<query>/ whose IDs say they were created in [lo, hi] (seconds)."""
    if lo > hi:
        return []  # nothing to search: TestGrid still shows everything since the earliest searchable time
    bucket, _, prefix = query.partition("/")
    prefix += "/"
    first = (max(0, int(lo * 1000) - PROW_EPOCH_MS)) << 22
    ids, token = [], None
    for _ in range(MAX_LIST_PAGES):
        params = {"prefix": prefix, "delimiter": "/", "startOffset": f"{prefix}{first}",
                  "fields": "prefixes,nextPageToken"}
        if token:
            params["pageToken"] = token
        page = get_json(f"{gcs}/storage/v1/b/{quote(bucket)}/o?{urllib.parse.urlencode(params)}")
        prefixes = page.get("prefixes", []) if isinstance(page, dict) else None
        if not isinstance(prefixes, list):
            raise IntegrityError(f"unexpected GCS listing for gs://{query}/")
        for p in prefixes:
            build = p[len(prefix):].rstrip("/") if isinstance(p, str) and p.startswith(prefix) else ""
            if BUILD_RE.fullmatch(build) and lo <= created(build) <= hi:
                ids.append(build)
        token = page.get("nextPageToken")
        if not token:
            return ids
    raise IntegrityError(f"the GCS listing of gs://{query}/ did not end after {MAX_LIST_PAGES} pages")


def scan(testgrid, gcs, dashboards, pool, deadline):
    """Read every tab of every dashboard.

    Returns (tab reports, red builds by (job, build), every build each query
    shows, builds shown as still running by (job, build)). A tab's report says
    how far back it shows results ("oldest") and how long its job can run
    ("max_job_hours", from prowjob.json when known).
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
    builds, visible, newest, running = {}, {}, {}, {}
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
        visible.setdefault(query, set()).update(shown)
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
    return reports, builds, visible, running


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


def download(url, path, compress=False, max_bytes=None):
    """Save url to path (gzipped to path.gz if compress), checking length and md5 against GCS;
    a file larger than max_bytes is stored as numbered parts instead (see place()).

    The returned md5 and byte count describe the raw content either way.
    """
    final = path + ".gz" if compress else path
    part = temp_path(final)

    def consume(r):
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
            parts = place(part, final, max_bytes)
        except OSError as e:
            raise LocalIOError(f"{final}: {e}") from None
        info = {"bytes": out_len, "md5": out.hexdigest(),
                "verified": gcs["bytes"] is not None and (gcs["md5"] is not None or crc_of is not None),
                "file": os.path.basename(final), "file_bytes": file_bytes, "gcs": gcs}
        return dict(info, parts=parts) if parts else info

    try:
        # Accept gzip so a gzip-stored object arrives exactly as stored and stays verifiable.
        return request(url, consume, headers={"Accept-Encoding": "gzip"})
    finally:
        if os.path.exists(part):
            os.remove(part)


def write_json(path, data):
    write_file(path, json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


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


def merge_tabs(old, new):
    """Old tab sightings plus any tab this build is newly red on. The first sighting
    wins, except that one taken from GCS in a history gap (judged by TestGrid's
    rule, or from before that, without red cells) takes the cells TestGrid shows later."""
    def from_gcs(t):
        return t.get("judged_from_gcs") or not t.get("red_cells")
    cells = {(t["dashboard"], t["tab"]): t for t in new if not from_gcs(t)}
    merged = [cells.get((t["dashboard"], t["tab"]), t) if from_gcs(t) else t for t in old]
    seen = {(t["dashboard"], t["tab"]) for t in old}
    added = [t for t in new if (t["dashboard"], t["tab"]) not in seen]
    return merged + added, bool(added) or merged != old


PART_STEP = 1 << 20  # parts are copied this much at a time


def remove_parts(target, name, keep=()):
    """Remove the stored parts of `name` in target, except the ones named in keep."""
    for n in listdir(target):
        m = PART_RE.fullmatch(n)
        if m and m.group(1) == name and n not in keep:
            remove(os.path.join(target, n))


def place(tmp, final, max_bytes=None):
    """Move a verified download from its temp file to `final`, or, if it is larger than
    max_bytes, to numbered parts final.000, final.001, ... of at most max_bytes each
    (GitHub rejects files over 100 MB): meta.json lists them, and `cat <file>.* > <file>`
    puts the file back together. A file too large for git so never sits under a name git
    commits. The new copy replaces the old one (whole or in parts) at once, after every
    part of it is written: a run abandoned, or a disk full, part-way through leaves the
    old copy as it was. Returns the parts ([] for a whole file)."""
    target, name = os.path.split(final)
    size = os.path.getsize(tmp)
    if max_bytes is None or size <= max_bytes:
        with context().write_lock:  # (replace() and remove() take it too)
            replace(tmp, final)
            remove_parts(target, name)  # (left from an earlier copy stored in parts)
        return []
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
        with context().write_lock:  # all of them or none: abandon() waits, and replace() checks it first
            for part_tmp, part in staged:
                replace(part_tmp, part)
            remove(final)  # (an earlier copy stored whole)
            remove_parts(target, name, keep={p["file"] for p in parts})
    finally:
        for part_tmp, _ in staged:
            if os.path.exists(part_tmp):
                os.remove(part_tmp)
    return parts


def save_log(base, target, compress, max_bytes):
    """Download build-log.txt, dropping a copy in the other format left by an earlier run."""
    log = download(base + "/build-log.txt", os.path.join(target, "build-log.txt"), compress, max_bytes)
    for stale in {"build-log.txt", "build-log.txt.gz"} - {log["file"]}:
        remove(os.path.join(target, stale))
        remove_parts(target, stale)
    return log


def stray_files(target, meta):
    """Stored-file names in a build folder that its meta.json does not list: what a copy
    cut off part-way leaves (parts of a newer copy, or a copy in the other form)."""
    listed = set()
    for default, key in (("build-log.txt", "log"), ("podinfo.json", "podinfo")):
        info = meta.get(key)
        if info and isinstance(info, dict) and not info.get("omitted"):  # (as intact() reads it)
            parts = info.get("parts")
            listed |= ({p.get("file") for p in parts if isinstance(p, dict)} if isinstance(parts, list)
                       else {info.get("file", default)})
    return sorted(n for n in listdir(target) if (n in LOG_FILES or PART_RE.fullmatch(n)) and n not in listed)


class RecheckError(Exception):
    """Comparing an intact archived build with GCS failed; its copy stays and is checked again next run."""


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


def read_artifact(url, limit):
    """An object's bytes as TestGrid's GCS client reads them (gunzipped when GCS
    serves them gzipped), None when there is no such object."""
    def consume(r):
        encoding = (r.headers.get("Content-Encoding") or "identity").strip().lower()
        if encoding == "identity":
            return read_body(r, limit)
        if encoding != "gzip":
            raise IntegrityError(f"{r.url}: unsupported Content-Encoding {encoding!r}")
        out, total = [], 0
        for piece in gunzipped(chunks(r, r.url)):
            total += len(piece)
            if total > limit:
                raise IntegrityError(f"{r.url}: more than the {limit} byte limit")
            out.append(piece)
        return b"".join(out)
    try:
        return request(url, consume=consume)
    except NotFound:
        return None


def list_objects(gcs, bucket, folder):
    """Every object under gs://bucket/folder, as TestGrid lists a build's artifacts:
    {name: (size, stored gzipped?)}."""
    out, token = {}, None
    for _ in range(MAX_LIST_PAGES):
        params = {"prefix": folder, "fields": "items(name,size,contentEncoding),nextPageToken"}
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
            out[name] = (int(size), str(item.get("contentEncoding") or "").strip().lower() == "gzip")
        token = page.get("nextPageToken")
        if not token:
            return out
    raise IntegrityError(f"the GCS listing of gs://{bucket}/{folder} did not end after {MAX_LIST_PAGES} pages")


def gcs_column(gcs, query, build, days=tr.DAYS_OF_RESULTS, now=None):
    """TestGrid's column for a build, from GCS read as its updater reads it (see
    testgrid_rule): started.json, finished.json, podinfo.json and, unless reading
    those failed (that error is then the column), every junit file it would parse.
    A build that started more than `days` (the tab's days of results) before it was
    made counts as too old, as TestGrid then calls it. Reads of those three files
    that fail raise; if the junit files cannot be listed or read, the column is the
    one without them, with .partial saying why (see verdict)."""
    base = f"{gcs}/{query}/{build}"
    job = query.rsplit("/", 1)[-1]
    files = [read_artifact(f"{base}/{name}", MAX_JSON_BYTES) for name in ("started.json", "finished.json", "podinfo.json")]
    now = time.time() if now is None else now
    stop = created(build) - int(days * 86400)
    col = tr.column(job, *files, [], stop, now)
    partial = None
    if not col.errors:
        bucket, _, path = query.partition("/")
        folder = f"{path}/{build}/"
        try:
            listing = list_objects(gcs, bucket, folder)
            junits = []
            for _, opened in tr.junit_reads(sorted(listing)):
                size, gzipped = listing.get(opened, (None, False))
                if size is None:
                    junits.append((opened, None, None))  # TestGrid cannot open it either
                    continue
                size = -1 if gzipped else size  # read decompressed, of unknown length
                data = b"" if size > tr.JUNIT_MAX_SIZE else read_artifact(
                    f"{gcs}/{bucket}/{urllib.parse.quote(opened, safe='/')}", MAX_JUNIT_BYTES)
                junits.append((opened, size, data))
            col = tr.column(job, *files, junits, stop, now, folder=folder)
        except (urllib.error.HTTPError, *TRANSIENT) as e:  # TestGrid read them then; they cannot be read now
            partial = f"its junit files cannot be read: {type(e).__name__}: {e}"[:300]
    col.has_finished, col.partial = files[1] is not None, partial
    return col


def unreadable_junit(col):
    """Why the column cannot be judged though nothing else keeps it undecided: its junit
    files could not be read, and no cell of it still runs. None otherwise."""
    return col.partial if getattr(col, "partial", None) and "RUNNING" not in (col.cells or {}).values() else None


def verdict(col, job, rows=None):
    """What TestGrid's column for the build shows on a tab: ("red", red rows), or,
    when it is not red there, ("undecided", []) while TestGrid would still read it
    again (not started, or a cell still RUNNING: the build, or its Pod row waiting
    for podinfo.json) or its junit files cannot be read, else ("not-red", []).
    rows: {"overall": shown?, "pod": shown?} for the tab (shown when not known). Of
    the columns readResult's racing reads can give, the build counts as red when
    any is."""
    rows = rows or {}
    if col.errors:
        if "TOOL_FAIL" in col.results:
            return "red", [{"test": "Overall", "status": "TOOL_FAIL", "message": "TestGrid could not read the build from GCS"}]
        if "UNKNOWN" in col.results or col.has_finished:
            return "not-red", []  # too old, or finished without ever starting: never red
        return "undecided", []
    if not col.red(job, rows.get("overall", True), rows.get("pod", True)):
        # TestGrid reads a column again while any cell of it runs (truncateRunning): the build,
        # or its Pod row waiting for podinfo.json; what it reads then can still turn it red.
        # And without its junit files a column is only as red as its job's rows: a failing
        # test could still make it red.
        running = "RUNNING" in col.cells.values()
        return ("undecided" if running or getattr(col, "partial", None) else "not-red"), []
    # Red now is red: what makes a cell red comes from objects GCS writes whole and once
    # (finished.json, podinfo.json, junit files), so reading them again keeps it red.
    hidden = {job + ".Overall"} if not rows.get("overall", True) else set()
    hidden |= {job + ".Pod"} if not rows.get("pod", True) else set()
    return "red", [{"test": row, "status": col.cells[row], "message": col.notes.get(row) or None}
                   for row in col.red_rows() if row not in hidden]


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


def settle_flags(meta, checked, now):
    """(settled, final) for an archived copy found equal to GCS at time `checked`.

    settled: the copy was taken or confirmed SETTLE_HOURS after the build
    finished, when Prow had stopped uploading. final: it was confirmed
    RECHECK_HOURS after that, so later runs stop comparing it. A build without
    finished.json, or without a log, keeps being checked for a late one until
    GIVE_UP_DAYS after it was first archived.
    """
    fin = meta.get("finished")
    if not plausible_time(fin, now) or fin < meta["created"] - 86400:
        fin = None  # not a time this build could have finished at
    first = meta.get("archived_at") if plausible_time(meta.get("archived_at"), now) else meta["created"]
    settled = fin is not None and checked >= fin + SETTLE_HOURS * 3600
    if fin is None or not meta.get("log"):
        return settled, checked - first >= GIVE_UP_DAYS * 86400
    return settled, checked >= fin + RECHECK_HOURS * 3600


def recheck(old, rec, target, base, run_id, now, compress, max_bytes):
    """An intact archived build: add new tab sightings and, until it is final,
    compare its files with GCS and fetch again whatever GCS now holds instead."""
    tabs, _ = merge_tabs(old["tabs"], rec["tabs"])
    meta = copy.deepcopy(dict(old, tabs=tabs))  # the file records may gain a GCS identity below
    if meta.get("started") is None and rec.get("started") is not None:
        meta["started"] = rec["started"]  # taken from GCS in a gap, then shown by TestGrid
    if meta != old:  # new sightings are recorded first: a GCS check that fails below must not lose them
        write_json(os.path.join(target, "meta.json"), meta)
        old = copy.deepcopy(meta)
    outcome = "already-archived"
    if not old.get("final"):
        try:
            finished = read_finished(base)
            changed = []
            if meta.get("log"):
                if not unchanged_in_gcs(base + "/build-log.txt", meta["log"]):
                    before = meta["log"].get("md5")
                    meta["log"] = save_log(base, target, compress, max_bytes)
                    if meta["log"]["md5"] != before:  # an old record may only lack GCS's identity
                        changed.append("build-log.txt")
                        meta.update(refreshed_by_run=run_id, refreshed=list(changed))
                    write_json(os.path.join(target, "meta.json"), meta)  # the file on disk changed: say so now
            else:
                try:  # archived without a log: has one appeared since?
                    meta["log"] = save_log(base, target, compress, max_bytes)
                    meta["log_recovered_by_run"] = run_id
                    outcome = "log-recovered"
                    write_json(os.path.join(target, "meta.json"), meta)  # the file on disk changed: say so now
                except NotFound:
                    pass
            if meta.get("podinfo") and not unchanged_in_gcs(base + "/podinfo.json", meta["podinfo"]):
                before = meta["podinfo"].get("md5")
                meta["podinfo"] = download(base + "/podinfo.json", os.path.join(target, "podinfo.json"),
                                           max_bytes=max_bytes)
                if meta["podinfo"]["md5"] != before:
                    changed.append("podinfo.json")
            elif not meta.get("log") and not meta.get("podinfo"):
                try:  # Prow writes podinfo.json up to minutes after finished.json
                    meta["podinfo"] = download(base + "/podinfo.json", os.path.join(target, "podinfo.json"),
                                               max_bytes=max_bytes)
                    changed.append("podinfo.json")
                except NotFound:
                    pass
        except (Abandoned, LocalIOError):
            raise
        except Exception as e:
            first = meta.get("archived_at") if plausible_time(meta.get("archived_at"), now) else meta["created"]
            if now - first <= GIVE_UP_DAYS * 86400:
                raise RecheckError(f"comparing the archived copy with GCS failed: {type(e).__name__}: {e}") from None
            meta.update(final=True, note=f"not compared with GCS after {GIVE_UP_DAYS} days of errors: "
                                         f"{type(e).__name__}: {e}"[:300])
            write_json(os.path.join(target, "meta.json"), meta)
            return "stopped-comparing", meta
        if changed:
            meta.update(refreshed_by_run=run_id, refreshed=changed)
            if outcome == "already-archived":
                outcome = "refreshed"
        if finished is not None:  # a field this read could not tell keeps what was recorded
            if finished["result"] is not None:
                meta["result"] = finished["result"]
            if finished["timestamp"] is not None:
                meta["finished"] = finished["timestamp"]
        meta["settled"], meta["final"] = settle_flags(meta, time.time(), now)
    if meta != old:
        write_json(os.path.join(target, "meta.json"), meta)
    return outcome, meta


def archive(rec, target, gcs, run_id, now, max_job_hours, compress=False, max_bytes=95 << 20, tab_rules=None):
    """Archive one red build into `target` (its existing folder, or one in this run's folder).
    tab_rules: per tab ("dashboard#tab"), the rows it shows and its days of results, for a
    build from a history gap that is judged here (see judge_in_tabs)."""
    meta_path = os.path.join(target, "meta.json")
    base = f"{gcs}/{rec['query']}/{rec['build']}"
    existed = os.path.exists(meta_path)
    try:
        old = read_json(meta_path)
    except ValueError:
        old = None  # corrupt meta.json: archive the build again
    old = old if valid_meta(old) and old.get("job") == rec["job"] and old.get("build") == rec["build"] else None
    if old and intact(target, old, deep=True):
        return recheck(old, rec, target, base, run_id, now, compress, max_bytes)
    if not rec.get("watch"):
        return store(rec, target, existed, old, read_finished(base), base, run_id, now, max_job_hours, compress,
                     max_bytes)
    # from a history gap, not decided when last looked at
    state, tabs, partial = judge_in_tabs(gcs, rec, tab_rules or {})
    if state != "red":
        if state == "not-red":
            return "not-red", rec  # finished (or never started, or too old) and not red on any of its tabs
        if partial:  # finished, and only its junit files keep it from being judged: an error
            raise IntegrityError(f"cannot be judged by TestGrid's rule: {partial}")
        return ("watching" if now - rec["created"] <= GIVE_UP_DAYS * 86400 else "stopped-watching"), rec
    rec = {k: v for k, v in rec.items() if k != "watch"}
    rec["tabs"] = tabs
    rec["backfilled"] = backfill_note(tabs, partial)
    try:
        return store(rec, target, existed, old, read_finished(base), base, run_id, now, max_job_hours, compress,
                     max_bytes)
    except Exception as e:
        e.red_rec = rec  # now known to be red: retried and given up on like any other red build
        raise


def judge_in_tabs(gcs, rec, tab_rules):
    """("red", its tabs with their red cells) when TestGrid's column for the build is
    red on one of the tabs in rec["tabs"], else ("not-red" | "undecided", []); and the
    column's .partial (why its junit files were not read, else None)."""
    rules = [tab_rules.get(f"{t['dashboard']}#{t['tab']}", {}) for t in rec["tabs"]]
    col = gcs_column(gcs, rec["query"], rec["build"], max([r.get("days", tr.DAYS_OF_RESULTS) for r in rules] or
                                                          [tr.DAYS_OF_RESULTS]))
    states, red = [], []
    for t, rule in zip(rec["tabs"], rules, strict=True):
        state, cells = verdict(col, rec["job"], rule.get("rows"))
        states.append(state)
        if state == "red":
            red.append(dict(t, red_cells=cells, judged_from_gcs=True))
    if red:
        return "red", red, getattr(col, "partial", None)
    return ("undecided" if "undecided" in states else "not-red"), [], unreadable_junit(col)


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


def store(rec, target, existed, old, finished, base, run_id, now, max_job_hours, compress, max_bytes):
    """Download a red build's files into `target` and write its meta.json."""
    meta_path = os.path.join(target, "meta.json")
    os.makedirs(target, exist_ok=True)
    podinfo = note = None
    try:
        try:
            log = save_log(base, target, compress, max_bytes)
        except NotFound:
            if old and old.get("log"):
                raise RuntimeError(f"the archived log is damaged and GCS no longer has it ({base})") from None
            age = now - (rec["started"] or rec["created"])
            if finished is None and age <= 2 * max_job_hours * 3600:
                remove_build_dir(target)
                return "pending", rec  # still uploading: look again next run
            # The build is over without a log: its pod never ran (keep Prow's pod
            # status), or TestGrid gave up on it ("did not complete within 24
            # hours") and GCS has nothing. Either way keep the record; later runs
            # look for a late log for GIVE_UP_DAYS.
            log = None
            if finished is None:
                note = f"GCS had neither finished.json nor build-log.txt {age / 3600:.0f}h after the build started"
            try:
                podinfo = download(base + "/podinfo.json", os.path.join(target, "podinfo.json"), max_bytes=max_bytes)
            except NotFound:
                pass
    except Exception:
        remove_build_dir(target)
        raise
    tabs = merge_tabs(old["tabs"], rec["tabs"])[0] if old else rec["tabs"]
    home = os.path.basename(os.path.dirname(os.path.dirname(target)))  # the run folder that holds it
    meta = dict(rec,
                tabs=tabs,
                result=(finished or {}).get("result"),
                finished=(finished or {}).get("timestamp"),
                prow_url=f"https://prow.k8s.io/view/gs/{rec['query']}/{rec['build']}",
                log_url=base + "/build-log.txt",
                log=log,
                podinfo=podinfo,
                archived_by_run=home,
                archived_at=folder_time(home) or now)
    if (old or {}).get("backfilled"):
        meta["backfilled"] = old["backfilled"]
    if note:
        meta["note"] = note
    if existed:
        meta["repaired_by_run"] = run_id
    meta["settled"], meta["final"] = settle_flags(meta, time.time(), now)
    write_json(meta_path, meta)
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
            elif real_dir(src) and all(n in LOG_FILES or n.endswith(".part") for n in os.listdir(src)):
                for n in os.listdir(src):  # a download the old code never finished: fetched again
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
    for old in sorted({m["archived_by_run"] for *_, m in builds} | set(reports), key=lambda o: started_of(o) or 0):
        if old not in folder_of and started_of(old) is not None:
            folder_of[old] = folder_for(old)
    known, _ = archived_builds(root)
    for src, job, build, meta in builds:
        old = meta["archived_by_run"]
        if old not in folder_of:
            warnings.append(f"old layout: logs/{job}/{build} names a run {old[:40]!r} with no known time; left in place")
            continue
        rid = folder_of[old]
        dest = os.path.join(runs, rid[:7], rid, job, build)
        if f"{job}/{build}" in known or os.path.exists(dest):
            where = known.get(f"{job}/{build}") or os.path.relpath(dest, root)
            warnings.append(f"old layout: logs/{job}/{build} is also archived at {where}; left in place")
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
    for run_rel in run_dirs(root):
        run_abs = os.path.join(root, run_rel)
        for job in listdir(run_abs):
            job_abs = os.path.join(run_abs, job)
            if job in RUN_FILES or not real_dir(job_abs):
                continue
            for build in listdir(job_abs):
                target = os.path.join(job_abs, build)
                if not real_dir(target):
                    continue
                if os.path.exists(os.path.join(target, "meta.json")):
                    try:
                        meta = read_json(os.path.join(target, "meta.json"))
                    except (ValueError, OSError):
                        continue  # reported as unreadable by the index
                    for n in stray_files(target, meta) if valid_meta(meta) else []:
                        try:
                            remove(os.path.join(target, n))
                            warnings.append(f"{run_rel}/{job}/{build}/{n}: removed, as its meta.json does not list "
                                            "it (a copy cut off part-way)")
                        except OSError as e:
                            warnings.append(f"{run_rel}/{job}/{build}/{n}: not listed in its meta.json, and "
                                            f"cannot be removed: {e}")
                    continue
                names = listdir(target)
                if all(n in LOG_FILES or PART_RE.fullmatch(n) or (n.startswith(".") and n.endswith(".part"))
                       for n in names):
                    for n in names:
                        remove(os.path.join(target, n))
                    remove_build_dir(target)
                else:
                    warnings.append(f"{run_rel}/{job}/{build}: no meta.json but unexpected files {sorted(names)[:5]}")
            if not listdir(job_abs):
                remove_if_empty(job_abs)
        # a run killed before it archived or reported anything leaves an empty folder
        if not listdir(run_abs):
            remove_if_empty(run_abs)
            remove_if_empty(os.path.dirname(run_abs))


INDEX_TESTS = 100  # red tests an index entry lists (meta.json has them all)


def index_entry(job, build, meta, now, rel=None):
    t = meta["started"] or meta["created"]
    if not plausible_time(t, now):
        raise ValueError(f"implausible time {t!r}")
    red_tests = sorted({c["test"] for tab in meta["tabs"] for c in tab["red_cells"]},
                       key=lambda name: (name == f"{job}.Overall", name.startswith(f"{job}."), name))
    job_rows = [name for name in red_tests if name.startswith(f"{job}.")]
    files = [((meta.get(key) or {}).get("file", name), meta.get(key))
             for name, key in (("build-log.txt", "log"), ("podinfo.json", "podinfo"))]
    base = (meta.get("log_url") or "").rsplit("/", 1)[0]
    remote = [(name.removesuffix(".gz") if name else default, f"{base}/{default}")
              for (name, info), default in zip(files, ("build-log.txt", "podinfo.json"), strict=True)
              if info and info.get("omitted") and base]
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
        "dir": rel,
        "run": rel.split("/")[2] if rel else None,
        "files": [f"{rel}/{part['file']}" for name, info in files if info and not info.get("omitted")
                  for part in info.get("parts") or [{"file": name}]] if rel else [],
        "remote": remote,
        "bytes": sum(info["bytes"] for _, info in files if info),
        "settled": meta.get("settled") if isinstance(meta.get("settled"), bool) else None,
        "final": meta.get("final") if isinstance(meta.get("final"), bool) else None,
        "backfilled": bool(meta.get("backfilled")),
        "has_log": bool(meta.get("log")),
        "in_parts": any((meta.get(key) or {}).get("parts") for key in ("log", "podinfo")),
        # red only by TestGrid's rule applied to GCS: TestGrid never showed it red to the archive
        "from_gcs": bool(meta.get("backfilled")) and all(t.get("judged_from_gcs") or not t.get("red_cells")
                                                         for t in meta["tabs"]),
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
        if e.get("from_gcs"):
            shown = (shown + " " if shown else "") + "(taken from GCS: red by TestGrid's rule, not seen red on TestGrid)"
        if files:
            links = [f"[{p.rsplit('/', 1)[1]}]({prefix}{p})" for p in e["files"]]
            links += [f"[{name} in GCS (too large to store)]({url})" for name, url in e.get("remote") or []]
            last = " ".join(links) or "none"
            if e.get("settled") is False:
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
    """Write path atomically, leaving it untouched when the content is the same."""
    try:
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


def run_rows(root, first_day, current=None):
    """Markdown rows for the run folders started on or after first_day, newest first."""
    rows = []
    for rel in reversed(run_dirs(root)):
        name = rel.rsplit("/", 1)[1]
        if dt.date.fromisoformat(name[:10]) < first_day:
            break
        if current and current.get("dir") == rel:
            run = current
        else:
            try:
                run = read_json(os.path.join(root, rel, "run.json"))
            except (ValueError, OSError):
                run = None
        if not isinstance(run, dict):
            rows.append(f"| [{run_title(name)}]({rel}/) | | | no run report: the run was cut off |")
            continue
        new, updated, problems = run_counts(run)
        result = "interrupted" if run.get("interrupted") is True else f"{problems} problems" if problems else "ok"
        rows.append(f"| [{run_title(name)}]({rel}/) | {new} | {updated} | {result} |")
    return rows


def write_run_readme(root, rel, run, entries):
    """README.md of a run folder: what the run did, and the builds it archived, with links."""
    name = rel.rsplit("/", 1)[1]
    up = "../../../"
    mine = [e for e in entries if (e.get("dir") or "").startswith(rel + "/")]
    lines = [f"# Run {run_title(name)} UTC", ""]
    if isinstance(run, dict):
        tabs = run.get("tabs") if isinstance(run.get("tabs"), list) else []
        failed = sum(1 for t in tabs if isinstance(t, dict) and t.get("error"))
        new, updated, problems = run_counts(run)
        seen = run.get("red_builds_seen") if is_int(run.get("red_builds_seen")) else "?"
        lines.append(f"{len(tabs)} tabs read ({failed} failed), {seen} red builds on "
                     f"TestGrid. This run archived {new} new builds" + (f" and updated {updated} archived earlier"
                                                                       if updated else "") + ".")
        if run.get("interrupted") is True:
            lines.append("The run was interrupted; unfinished builds are retried by the next run.")
    else:
        lines.append("This run was cut off before it wrote its report.")
    lines += ["", f"[run.json](run.json) has the details; [INDEX.md]({up}INDEX.md) lists every build. "
              "A file marked provisional was taken before Prow finished uploading and is re-checked against "
              "GCS on every run until it settles.", ""]
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
        problems = [f"{md_cell(t.get('dashboard'))}#{md_cell(t.get('tab'))}: {md_cell(t['error'])}"
                    for t in (run.get("tabs") if isinstance(run.get("tabs"), list) else [])
                    if isinstance(t, dict) and t.get("error")]
        problems += [f"{md_cell(e.get('build'))}: {md_cell(e.get('error'))}"
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
            ("… provisional (taken before Prow finished uploading)",
             count(lambda e: e["settled"] is False and e["final"] is not True)),
            ("… still compared with GCS on every run", count(lambda e: e["final"] is not True)),
            ("… taken from GCS, judged by TestGrid's rule (not seen red on TestGrid)", count(lambda e: e["backfilled"])),
            ("Red builds waiting to be archived", sum(1 for e in waiting if not e["status"].startswith("given up"))),
            ("… of them with a damaged copy here, to be fetched again",
             sum(1 for e in waiting if e["on_disk"] and not e["status"].startswith("given up"))),
            ("Red builds given up on (after 14 days)", sum(1 for e in waiting if e["status"].startswith("given up"))),
            ("Builds watched until TestGrid's rule can judge them (still running)", watched)]
    return (["## What the archive holds", "", "| | Builds |", "|---|---:|"]
            + [f"| {md_cell(what)} | {n} |" for what, n in rows])


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
             f"Updated {iso(now)}. Every build with a red cell on "
             f"{' and '.join(DASHBOARDS)}; times are UTC. Each run's downloads are in its own folder under "
             "[runs/](runs/). A file marked provisional was taken before Prow finished uploading; it is "
             "compared with GCS on every run and fetched again if GCS changes.", ""]
    lines += report.write_charts(root, counted, now, current_run)
    if waiting:
        lines += ["## Not archived yet", "", f"{len(waiting)} red builds without a verified local copy yet."]
        lines += build_rows(waiting, files=False) + [""]
    lines += [f"## Runs in the last {report.HEAT_DAYS} days", "",
              "| Run (UTC) | New builds | Updated | Result |", "|---|---:|---:|---|"]
    lines += run_rows(root, first_day, current_run) + [""]
    lines += [f"## Builds from the last {report.HEAT_DAYS} days", "",
              f"{len(recent)} archived builds that started since {first_day.isoformat()} (UTC), newest first."]
    lines += build_rows(recent) + [""] + holdings(entries, waiting, current_run) + ["", "## Every week", ""]
    lines += [f"- [{w['week']}]({w['page']}): {short(dt.date.fromisoformat(w['from']))} – "
              f"{short(dt.date.fromisoformat(w['to']))}, {w['builds']} builds" for w in listing]
    write_text(os.path.join(root, "INDEX.md"), "\n".join(lines) + "\n")
    # Recent runs' READMEs show their builds' current state (a provisional file can settle later).
    horizon = now - RECHECK_HOURS * 3600 - 86400
    for rel in run_dirs(root):
        if current_run and current_run.get("dir") == rel:
            write_run_readme(root, rel, current_run, entries)
        elif folder_time(rel.rsplit("/", 1)[1]) >= horizon:
            try:
                run = read_json(os.path.join(root, rel, "run.json"))
            except (ValueError, OSError):
                run = None
            write_run_readme(root, rel, run, entries)
    return entries, bad


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


def load_state(root, warnings):
    try:
        state = read_json(os.path.join(root, "state.json"))
    except ValueError as e:
        warnings.append(f"state.json is corrupt ({e}); starting without it")
        state = {}
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
        else:
            warnings.append(f"state.json: dropped a malformed tabs entry {str(k)[:80]!r}")
    # Older archives also kept "no_log" here; the builds' own meta.json now says what to re-check.
    return {"tabs": tabs, "unresolved": entries("unresolved"), "given_up": entries("given_up"),
            "watch": entries("watch")}


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


def archived_intact(target, deep=False):
    try:
        meta = read_json(os.path.join(target, "meta.json"))
    except (ValueError, OSError):
        return False
    return valid_meta(meta) and intact(target, meta, deep)


def add_sightings(target, rec, run_id):
    """For a build whose worker the run gave up on, but whose copy is intact: merge rec's red
    tab sightings into its meta.json (so one from a gap search is kept), and say what this
    run did to it, as its meta.json records."""
    try:
        meta = read_json(os.path.join(target, "meta.json"))
        tabs, changed = merge_tabs(meta["tabs"], [t for t in rec["tabs"] if t.get("red_cells") or t.get("red_cells_omitted")])
        if changed:
            write_json(os.path.join(target, "meta.json"), dict(meta, tabs=tabs))
    except (ValueError, OSError, KeyError, TypeError, AttributeError):
        return "already-archived"  # (an unreadable meta.json is reported by the index)
    if meta.get("archived_by_run") == run_id:
        new = "backfilled" if meta.get("backfilled") else "archived" if meta.get("log") else "archived-without-log"
        return "repaired" if meta.get("repaired_by_run") == run_id else new
    for key, outcome in (("repaired_by_run", "repaired"), ("log_recovered_by_run", "log-recovered"),
                         ("refreshed_by_run", "refreshed")):
        if meta.get(key) == run_id:
            return outcome
    return "already-archived"


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


def backfill_gaps(gaps, gcs, pool, deadline, visible, skip, known=frozenset(), tab_rules=None, watched=None):
    """For each tab with a history gap, the builds GCS has from that time whose
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
    out, candidates = {}, []
    listings = {key: pool.submit(list_build_ids, gcs, r["query"], floor - BACKFILL_SLACK_HOURS * 3600, r["oldest"])
                for key, r, floor in gaps}
    cf.wait(listings.values(), timeout=max(0, deadline - time.monotonic()))
    for key, r, _ in gaps:
        fut = listings[key]
        if not fut.done():
            out[key] = TimeoutError("run timeout reached")
            continue
        if fut.exception():
            out[key] = fut.exception()
            continue
        out[key] = []
        job = r["query"].rsplit("/", 1)[-1]
        for b in fut.result():
            name = f"{job}/{b}"
            if b in visible.get(r["query"], ()):
                continue
            if name in watched:
                tabs = watched[name]["tabs"]
                if all((t["dashboard"], t["tab"]) != (r["dashboard"], r["tab"]) for t in tabs):
                    tabs.append({"dashboard": r["dashboard"], "tab": r["tab"], "tab_status": r.get("status"),
                                 "red_cells": [], "judged_from_gcs": True})
            elif name not in skip or name in known:
                candidates.append((key, r, b))
    days = {}
    for key, r, b in candidates:
        days[(r["query"], b)] = max(days.get((r["query"], b), 0), tab_rules.get(key, {}).get("days", tr.DAYS_OF_RESULTS))
    looked_up = {qb: pool.submit(gcs_column, gcs, qb[0], qb[1], d) for qb, d in days.items()}
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
            state, cells = verdict(fut.result(), job, tab_rules.get(key, {}).get("rows"))
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
                 "red_cells": cells if state == "red" else [], "judged_from_gcs": True}
                for _, r, _, state, cells, _ in seen]
        red = [t for t, (*_, state, _, _) in zip(tabs, seen, strict=True) if state == "red"]
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
    p.add_argument("--dashboard", action="append", dest="dashboards", help=f"default: {' '.join(DASHBOARDS)}")
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
    p.add_argument("--list-only", action="store_true", help="print the red builds without downloading")
    p.add_argument("--testgrid", default=TESTGRID)
    p.add_argument("--gcs", default=GCS)
    args = p.parse_args(argv)

    run_start = time.time()
    root = args.archive
    lock = None
    if not args.list_only:
        lock = lock_archive(root)
        if lock is None:
            print(f"ERROR another run holds {os.path.join(root, '.lock')}", file=sys.stderr)
            return 2
    deadline = time.monotonic() + args.run_timeout_minutes * 60
    ctx = RunContext(AdaptiveLimit(args.start_concurrency, args.max_concurrency), deadline)
    bind_context(ctx)
    # keep a kept-alive connection for every request that can be in flight (pools made from now on)
    HTTP.connection_pool_kw["maxsize"] = max(HTTP.connection_pool_kw["maxsize"], ctx.limit.ceiling)
    pool = cf.ThreadPoolExecutor(ctx.limit.ceiling, initializer=bind_context, initargs=(ctx,))  # (shared by the origins)
    # The GitHub runner ends a timed-out or cancelled step with SIGINT, then SIGTERM;
    # handle both the same way.
    previous_term = signal.signal(signal.SIGTERM, interrupt)
    try:
        return run(args, root, run_start, pool, deadline)
    except KeyboardInterrupt:  # outside the download wait (see run()): nothing to record yet
        ctx.abandon()  # workers stop at their next read and cannot write
        print("ERROR interrupted; unfinished builds are picked up by the next run", file=sys.stderr)
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        # Workers still blocked past the deadline are abandoned: they cannot write
        # any more (see replace()), and the process exit (os._exit, below) does not
        # wait for them.
        pool.shutdown(wait=not ctx.abandoned.is_set(), cancel_futures=True)
        if lock:
            lock.close()


def interrupt(signum, frame):
    raise KeyboardInterrupt


def run(args, root, run_start, pool, deadline):
    ctx = context()
    warnings = []
    if not args.list_only:
        sweep_parts(root)
        migrate_old_layout(root, warnings)
        clean_partial_builds(root, warnings)
    state = load_state(root, warnings)
    known, duplicates = archived_builds(root)
    folders = run_dirs(root)
    archive_start = folder_time(folders[0].rsplit("/", 1)[1]) if folders else None
    dashboards = args.dashboards or DASHBOARDS
    reports, builds, visible, running = scan(args.testgrid, args.gcs, dashboards, pool, deadline)
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
    read_ok, gaps = {}, []
    for r in reports:
        if r.get("tab") is None or r.get("error"):
            continue
        key = f"{r['dashboard']}#{r['tab']}"
        prev = state["tabs"].get(key)
        horizon = run_start - GIVE_UP_DAYS * 86400  # a gap is searched this far back at most
        first = prev is None and archive_start is not None
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
            if r["oldest"] > floor:
                if floor < horizon and not first:
                    warnings.append(f"{key}: the gap in TestGrid's history reaches back to {iso(floor)}; red builds "
                                    f"before {iso(horizon)} may be missing, as GCS is searched {GIVE_UP_DAYS} days back")
                gaps.append((key, r, max(floor, horizon)))
        if r.get("oldest"):
            read_ok[key] = {"scan": run_start, "oldest": r["oldest"], "margin": r["max_job_hours"], "query": r["query"],
                            **job_hours.get(key, {}), **({"rows": r["rows"]} if known_rows(r.get("rows")) else {})}
    summaries_ok = {r["dashboard"] for r in reports if r.get("tab") is not None}
    listed = {f"{r['dashboard']}#{r['tab']}" for r in reports if r.get("tab") is not None}
    tab_scans = {}
    for key, prev in sorted(state["tabs"].items()):
        if key.split("#", 1)[0] in summaries_ok and key not in listed:
            if not prev.get("missing_since"):
                warnings.append(f"{key}: no longer listed on its dashboard")
                prev = dict(prev, missing_since=run_start)
            elif run_start - prev["missing_since"] > UNLISTED_DAYS * 86400:
                warnings.append(f"{key}: unlisted for {UNLISTED_DAYS} days; forgetting it")
                continue
        tab_scans[key] = prev
    tab_scans.update(read_ok)  # a tab listed again loses its missing_since

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
    shown_now = {f"{q.rsplit('/', 1)[-1]}/{b}" for q, bs in visible.items() for b in bs}
    running_now = {f"{job}/{build}" for job, build in running}
    watched = {k: u for k, u in state["watch"].items()
               if k not in chosen and k not in given_up and k not in state["unresolved"] and k not in known}
    retry += [u["rec"] for k, u in watched.items() if k not in shown_now]
    # shown, no longer running and not red (it would be chosen): TestGrid has decided it, unless
    # a tab that may show it red could not be read now (one of its job, as last read, or any
    # tab when a dashboard's summary, which lists them, could not be read)
    unread = {(state["tabs"].get(f"{r['dashboard']}#{r['tab']}") or {}).get("query") if r.get("tab") is not None
              else None for r in reports if r.get("error")}
    shown_settled = sorted(k for k, u in watched.items() if k in shown_now and k not in running_now
                           and None not in unread and u["rec"]["query"] not in unread)
    still_shown = [u["rec"] for k, u in watched.items() if k in shown_now and k not in shown_settled]
    # Archived builds that are not final yet are compared with GCS again, even
    # after TestGrid drops them: their logs may still be replaced by Prow.
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
        if valid_meta(meta) and not meta.get("final") and (rec := recheck_rec(meta, run_start, key)):
            retry.append(rec)
            rechecks += 1
    # A gap in a tab's history: list the job's builds from that time in GCS and
    # archive the ones TestGrid painted red by its own rule, since it no longer shows them.
    backfill_errors, backfilled, new_watch, taken, running_gap = [], 0, {}, set(), []

    def last_scan(key):
        return (state["tabs"].get(key) or {"scan": archive_start})["scan"]
    if gaps and not args.list_only:
        # Builds retried anyway are not taken again: one known to be red (waiting to be
        # archived) is only judged for the gap's tab, and one watched gains that tab.
        in_retry = {f"{x['job']}/{x['build']}": x for x in retry}
        backfill = backfill_gaps(gaps, args.gcs, pool, deadline, visible, set(known) | queued,
                                 known=(set(known) - queued) | (in_retry.keys() & set(state["unresolved"])),
                                 tab_rules=tab_rules, watched={k: x for k, x in in_retry.items() if x.get("watch")})
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
        for key, r, floor in gaps:
            found = backfill.get(key)
            if isinstance(found, Exception):
                backfill_errors.append({"build": None, "error": f"{key}: listing GCS for the history gap failed: "
                                                               f"{type(found).__name__}: {found}"})
                # Keep the old scan time (none for a tab not scanned before), so the next run
                # searches this gap again; it never searches more than GIVE_UP_DAYS back.
                if key in state["tabs"]:
                    old = state["tabs"][key]
                    tab_scans[key] = dict(job_hours.get(key, {}), scan=old["scan"], oldest=old["oldest"],
                                          query=r["query"], **({"margin": old["margin"]} if "margin" in old else {}),
                                          **({"rows": r["rows"]} if known_rows(r.get("rows")) else {}))
                else:
                    tab_scans.pop(key, None)
                outcome = "looking them up in GCS failed; the next run tries again"
            else:
                new = [rec for rec in found if f"{rec['job']}/{rec['build']}" not in taken]  # also in another tab's gap
                taken.update(f"{rec['job']}/{rec['build']}" for rec in new)
                retry += new
                backfilled += len(new)
                undecided = sum(1 for w in new_watch.values()
                                if any(f"{t['dashboard']}#{t['tab']}" == key for t in w["tabs"]))
                warnings.append(f"{key}: TestGrid only shows builds since {iso(r['oldest'])}, but the last scan "
                                f"({iso(last_scan(key))}) needed them from {iso(floor)}; GCS was "
                                f"searched instead, and {len(found)} red builds from that time were taken from it"
                                + (f" ({undecided} not decided yet: still running, or unreadable and looked at "
                                   "again)" if undecided else ""))
                continue
            warnings.append(f"{key}: TestGrid only shows builds since {iso(r['oldest'])}, but the last scan "
                            f"({iso(last_scan(key))}) needed them from {iso(floor)}; red builds in between "
                            f"may be missing; {outcome}")
    elif gaps:
        warnings += [f"{key}: TestGrid only shows builds since {iso(r['oldest'])}, but the last scan "
                     f"({iso(last_scan(key))}) needed them from {iso(floor)}; red builds in between "
                     "may be missing" for key, r, floor in gaps]
    tab_errors = [r for r in reports if r.get("error")]

    if args.list_only:
        for b in sorted(selected + retry, key=lambda b: b["started"] or b["created"]):
            print(iso(b["started"] or b["created"]), b["job"], b["build"], json.dumps([t["tab"] for t in b["tabs"]]))
        print(f"{len(reports)} tabs read ({len(tab_errors)} failed), {len(selected)} red builds, "
              f"{len(retry) - rechecks} to retry, {rechecks} archived builds to compare with GCS again")
        for r in tab_errors:
            print("ERROR", r["dashboard"], r["tab"], r["error"], file=sys.stderr)
        for w in warnings:
            print("WARNING", w, file=sys.stderr)
        return 1 if tab_errors else 0

    run_id, run_rel = new_run_dir(root, run_start)
    outcomes, errors, downloaded, unresolved, watch = {}, list(backfill_errors), 0, {}, {}
    # judged by the gap search just now and still running, overdue on TestGrid, or watched and still
    # shown by TestGrid: watched without a look now, for as long as a build is ever watched
    for w in running_gap + list(stalled.values()) + still_shown:
        key = f"{w['job']}/{w['build']}"
        if run_start - w["created"] > GIVE_UP_DAYS * 86400:
            outcomes.setdefault("stopped-watching", []).append(key)
            continue
        outcomes.setdefault("watching", []).append(key)
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
            elif b.get("watch"):
                watch[key] = {"rec": b, "first_seen": state["watch"].get(key, {}).get("first_seen", run_start)}
            else:
                previous = state["unresolved"].get(key, {})
                unresolved[key] = {"rec": b, "first_seen": previous.get("first_seen", run_start),
                                   "last_error": "TimeoutError: run timeout reached before downloads started"}
                errors.append({"build": key, "error": "TimeoutError: run timeout reached before downloads started"})
    else:
        futures = {pool.submit(archive, b, target_of(b), args.gcs, run_id, run_start, hours(b), args.gzip,
                               max_bytes, tab_rules): b
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
        ctx.abandon()
    for fut in futures:
        b = futures[fut]
        key = f"{b['job']}/{b['build']}"
        previous = state["unresolved"].get(key, {})
        try:
            if fut in not_done:
                if archived_intact(target_of(b), deep=True):
                    # Finished just as the run gave up, or already on disk; one
                    # that is not final yet is compared with GCS next run.
                    outcomes.setdefault(add_sightings(target_of(b), b, run_id), []).append(key)
                    continue
                cause = "interrupted" if interrupted else "run timeout reached"
                raise TimeoutError(f"{cause} before this build was done")
            outcome, meta = fut.result()
            if outcome in ("archived", "archived-without-log") and meta.get("backfilled"):
                outcome = "backfilled"
            outcomes.setdefault(outcome, []).append(key)
            if outcome in ("archived", "archived-without-log", "backfilled", "repaired", "log-recovered", "refreshed"):
                downloaded += sum(meta[k]["bytes"] for k in ("log", "podinfo") if meta.get(k))
            if outcome == "pending":  # (a watched build found red is queued as red from now on)
                unresolved[key] = {"rec": meta, "first_seen": previous.get("first_seen", run_start), "last_error": None}
            elif outcome == "watching":
                watch[key] = {"rec": b, "first_seen": state["watch"].get(key, {}).get("first_seen", run_start)}
        except RecheckError as e:
            # The archived copy stays as it is; it is not final, so the next run compares it again.
            errors.append({"build": key, "error": str(e)})
        except Exception as e:
            message = f"{type(e).__name__}: {e}"
            if b.get("watch") and not getattr(e, "red_rec", None):
                # not known to be red yet: keep watching rather than list it as missing,
                # as long as a build is ever watched
                if run_start - b["created"] <= GIVE_UP_DAYS * 86400:
                    watch[key] = {"rec": b, "first_seen": state["watch"].get(key, {}).get("first_seen", run_start)}
                else:
                    message = f"no longer watched after {GIVE_UP_DAYS} days, as it could not be judged: {message}"
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
    warnings += [f"{key}: still not decided {GIVE_UP_DAYS} days after it was made (not finished, or TestGrid still "
                 "shows it running, or it has no usable start time, so TestGrid never paints it red); no longer watched"
                 for key in outcomes.get("stopped-watching", [])]

    timings["downloads"] = round(time.time() - run_start - sum(timings.values()), 2)  # (with the gap search)
    # A possible gap in TestGrid history, a tab gone from its dashboard or a lost
    # retry list means builds may be missing without an error naming them.
    serious = [w for w in warnings if any(k in w for k in SERIOUS)]
    report_ = {"run": run_id, "dir": run_rel, "started": run_start, "since_hours": args.since_hours,
               "max_job_hours": args.max_job_hours, "tabs": reports, "red_builds_seen": len(builds),
               "retried_from_state": len(retry) - rechecks - backfilled - (len(new_watch) - len(running_gap)),
               "rechecked": rechecks, "watched": len(watch),
               "backfilled_from_gcs": len(outcomes.get("backfilled", [])), "outcomes": outcomes,
               "errors": errors, "warnings": warnings, "serious_warnings": serious, "interrupted": interrupted}
    run_json = os.path.join(root, run_rel, "run.json")
    # Save what the next run needs before drawing the pages and charts: a failure
    # there must not lose a build that is about to scroll off TestGrid.
    write_json(os.path.join(root, "state.json"),
               {"tabs": tab_scans, "unresolved": unresolved, "given_up": given_up, "watch": watch})
    write_json(run_json, dict(report_, errors=errors + [{"build": None, "error": "the run stopped before it finished"}]))
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
    write_json(run_json, final)
    if entries is not None:
        write_run_readme(root, run_rel, final, entries)

    counts = [f"{k}={len(v)}" for k, v in sorted(outcomes.items())] + [f"errors={len(errors)}"]
    print(f"run {run_id}: {len(reports)} tabs read ({len(tab_errors)} failed), {len(builds)} red builds on TestGrid, "
          f"{len(selected)} selected, {len(retry) - rechecks - backfilled - (len(new_watch) - len(running_gap))} retried, "
          f"{rechecks} re-compared with GCS, "
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
        print("ERROR tab", r["dashboard"], r["tab"], r["error"], file=sys.stderr)
    for e in errors:
        print("ERROR build" if e["build"] else "ERROR", e["build"] or "", e["error"], file=sys.stderr)
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
    if context().abandoned.is_set():
        os._exit(code)  # do not wait for workers abandoned at the run timeout
    sys.exit(code)
