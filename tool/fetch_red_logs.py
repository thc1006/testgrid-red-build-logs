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
import itertools
import json
import math
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

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


# URLError, socket timeouts, resets and SSL errors are all OSError.
TRANSIENT = (OSError, http.client.HTTPException, IntegrityError)


def cpu_count():
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


class AdaptiveLimit:
    """Caps in-flight HTTP requests and retunes the cap while running (AIMD).

    The work is network-bound (a run keeps 4 cores under 40% busy), so speed
    comes from how many requests are in flight, not from extra processes.
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
                    self.cap += 1
                    self._streak = 0
                    self.peak = max(self.peak, int(self.cap))
            self._cond.notify_all()


class RunContext:
    """What one run's threads share: its request limiter, abandon flag and deadline.

    main() binds a fresh context to the main thread and to every worker of its
    pool, so a later run in the same process never re-arms or shares an earlier
    run's abandoned workers.
    """

    def __init__(self, limit, deadline=math.inf):
        self.limit = limit
        self.abandoned = threading.Event()
        self.deadline = deadline  # time.monotonic() by which the run stops waiting


_local = threading.local()
_DEFAULT_CONTEXT = RunContext(AdaptiveLimit(8, 8))


def context():
    return getattr(_local, "context", _DEFAULT_CONTEXT)


def bind_context(ctx):
    _local.context = ctx


class SameHostRedirects(urllib.request.HTTPRedirectHandler):
    """Follow a redirect only to the same scheme and host; another host is an HTTP error."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        old, new = urllib.parse.urlsplit(req.full_url), urllib.parse.urlsplit(urllib.parse.urljoin(req.full_url, newurl))
        if (old.scheme, old.netloc) != (new.scheme, new.netloc):
            raise urllib.error.HTTPError(req.full_url, code, f"refused redirect to another host ({new.netloc})",
                                         headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


OPENER = urllib.request.build_opener(SameHostRedirects)


def deadline_for(r):
    size = r.headers.get("Content-Length")
    return time.monotonic() + BASE_DEADLINE + (int(size) / MIN_RATE if size and size.isdigit() else 0)


def chunks(r, url):
    """Yield the body in small reads, failing once it falls behind its deadline."""
    end, abandoned = deadline_for(r), context().abandoned
    read = getattr(r, "read1", r.read)
    while chunk := read(64 << 10):
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
    if not value.strip().isascii() or not value.strip().isdigit():
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
    # http.client returns short reads instead of raising when a body is cut off.
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
    except (TypeError, ValueError, IndexError):
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
    for attempt in range(RETRIES):
        if ctx.abandoned.is_set():
            raise Abandoned(url)
        ctx.limit.acquire()
        pushback, asked = False, None
        try:
            req = urllib.request.Request(url, headers=headers or {}, method=method)
            with OPENER.open(req, timeout=timeout) as r:
                return consume(r)
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
            ctx.limit.release(pushback)
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


def table_rows(table):
    """Validate a table's shape and run-length encoding; return (width, rows).

    Each row is (name, [(value, count), ...], messages). Anything TestGrid would
    never send is rejected rather than guessed at: a wrong type anywhere, a
    count that is not a positive int, a status outside TestGrid's TestStatus
    values, a row that does not span the table exactly, or a build listed twice.
    """
    if not isinstance(table, dict):
        raise ValueError(f"table is a {type(table).__name__}, not an object")
    changelists, stamps = table.get("changelists"), table.get("timestamps")
    if not isinstance(changelists, list) or not isinstance(stamps, list):
        raise ValueError("changelists and timestamps must be lists")
    width = len(changelists)
    if len(stamps) != width:
        raise ValueError(f"{width} changelists but {len(stamps)} timestamps")
    if len(set(map(str, changelists))) != width:
        raise ValueError("a build is listed in two columns")
    tests = table.get("tests", [])
    if not isinstance(tests, list):
        raise ValueError("tests must be a list")
    rows = []
    for test in tests:
        if not isinstance(test, dict) or not is_text(test.get("name"), 1000):
            raise ValueError(f"malformed row {str(test)[:80]!r}")
        name, runs = test["name"], test.get("statuses")
        if not isinstance(runs, list):
            raise ValueError(f"row {name!r}: statuses must be a list")
        pairs, total = [], 0
        for r in runs:
            if not isinstance(r, dict):
                raise ValueError(f"row {name!r}: malformed run {str(r)[:80]!r}")
            value, count = r.get("value"), r.get("count")
            if not is_int(count) or count <= 0:
                raise ValueError(f"row {name!r}: run length {count!r} is not a positive integer")
            if not is_int(value) or value not in STATUSES:
                raise ValueError(f"row {name!r}: unknown status {value!r}")
            pairs.append((value, count))
            total += count
        if total != width:
            raise ValueError(f"row {name!r} spans {total} columns, not {width}")
        messages = test.get("messages")
        if messages is None:
            messages = []
        if not isinstance(messages, list) or not all(is_text(m, 1 << 20) for m in messages):
            raise ValueError(f"row {name!r}: messages must be a list of strings")
        rows.append((name, pairs, messages))
    return width, rows


def red_columns(table, rows=None):
    """Map column index -> red cells in that column (one column is one build).

    Rows are run-length encoded. `messages` only has entries for cells with a
    result, so a cell's message index skips the NO_RESULT cells before it.
    """
    if rows is None:
        _, rows = table_rows(table)
    reds = {}
    for name, runs, messages in rows:
        aligned = len(messages) == sum(count for value, count in runs if value != NO_RESULT)
        col = idx = 0
        for value, count in runs:
            if value in RED:
                for k in range(count):
                    reds.setdefault(col + k, []).append({
                        "test": name,
                        "status": RED[value],
                        "message": messages[idx + k] if aligned else None,
                    })
            col += count
            if value != NO_RESULT:
                idx += count
    return reds


def read_tab(table, now):
    """Validate one tab's table; return (query, job, oldest start, [(build, start, cells)],
    the builds whose columns hold results)."""
    _, rows = table_rows(table)
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
    informative = set()
    for _, runs, _ in rows:
        col = 0
        for value, count in runs:
            if value not in (NO_RESULT, 6):
                informative.update(range(col, col + count))
            col += count
    known = [s for i, s in enumerate(starts) if s is not None and i in informative]
    shown = {table["changelists"][i] for i in informative}
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
    shows). A tab's report says how far back it shows results ("oldest") and
    how long its job can run ("max_job_hours", from prowjob.json when known).
    """
    now, abandoned = time.time(), context().abandoned
    tabs, reports = [], []
    summaries = {d: pool.submit(get_json, f"{testgrid}/{quote(d)}/summary") for d in dashboards}
    cf.wait(summaries.values(), timeout=max(0, deadline - time.monotonic()))
    for dashboard, fut in summaries.items():
        try:
            if not fut.done():
                abandoned.set()
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
        abandoned.set()
    builds, visible, newest = {}, {}, {}
    for (dashboard, tab, status), fut in zip(tabs, futures, strict=True):
        report_ = {"dashboard": dashboard, "tab": tab, "status": status}
        reports.append(report_)
        try:
            if not fut.done():
                raise TimeoutError("run timeout reached before the table loaded")
            query, job, oldest, found, shown = read_tab(fut.result(), now)
            for build, _, _ in found:
                other = builds.get((job, build))
                if other and other["query"] != query:
                    raise ValueError(f"{job}/{build} is also listed under {other['query']}")
        except Exception as e:
            report_["error"] = f"{type(e).__name__}: {e}"
            continue
        columns = fut.result()["changelists"]
        report_.update(query=query, columns=len(columns), oldest=oldest, red_builds=len(found))
        visible.setdefault(query, set()).update(shown)
        if columns:
            newest[id(report_)] = pool.submit(prow_job_hours, gcs, query, columns[0])
        for build, started, cells in found:
            rec = builds.setdefault((job, build), {"job": job, "build": build, "query": query,
                                                   "started": started, "created": created(build), "tabs": []})
            rec["tabs"].append({"dashboard": dashboard, "tab": tab, "tab_status": status, "red_cells": cells})
    cf.wait(newest.values(), timeout=max(0, min(deadline - time.monotonic(), PROWJOB_WAIT)))
    for r in reports:
        fut = newest.get(id(r))
        if fut and fut.done() and fut.exception() is None and fut.result():
            r["max_job_hours"] = fut.result()
    return reports, builds, visible


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
    refuse_if_abandoned(dst)
    os.replace(src, dst)


def download(url, path, compress=False):
    """Save url to path (gzipped to path.gz if compress), checking length and md5 against GCS.

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
            replace(part, final)
            file_bytes = os.path.getsize(final)
        except OSError as e:
            raise LocalIOError(f"{final}: {e}") from None
        return {"bytes": out_len, "md5": out.hexdigest(),
                "verified": gcs["bytes"] is not None and (gcs["md5"] is not None or crc_of is not None),
                "file": os.path.basename(final), "file_bytes": file_bytes, "gcs": gcs}

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


def file_digest(path):
    """(md5, length) of a file's raw content; a .gz file is gunzipped strictly, in bounded steps."""
    md5, size = hashlib.md5(), 0
    with open(path, "rb") as f:
        data = iter(lambda: f.read(1 << 20), b"")
        for chunk in gunzipped(data) if path.endswith(".gz") else data:
            md5.update(chunk)
            size += len(chunk)
    return md5.hexdigest(), size


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
                    continue  # verified but deliberately not stored (too large for git)
                name = info.get("file", default)
                if name not in LOG_FILES:
                    return False
                path = os.path.join(target, name)
                if os.path.getsize(path) != info.get("file_bytes", info["bytes"]):
                    return False
                if deep and file_digest(path) != (info["md5"], info["bytes"]):
                    return False
            except (OSError, IntegrityError, KeyError, TypeError, AttributeError):
                return False
    return True


def valid_meta(meta):
    return (isinstance(meta, dict) and isinstance(meta.get("tabs"), list)
            and all(isinstance(t, dict) and "dashboard" in t and "tab" in t for t in meta["tabs"]))


def merge_tabs(old, new):
    """Old tab sightings plus any tab this build is newly red on. The first sighting
    wins, except that one without red cells (taken from GCS in a history gap)
    takes the cells TestGrid shows later."""
    cells = {(t["dashboard"], t["tab"]): t for t in new if t.get("red_cells")}
    merged = [cells.get((t["dashboard"], t["tab"]), t) if not t.get("red_cells") else t for t in old]
    seen = {(t["dashboard"], t["tab"]) for t in old}
    added = [t for t in new if (t["dashboard"], t["tab"]) not in seen]
    return merged + added, bool(added) or merged != old


def keep_or_omit(info, target, max_bytes):
    """Drop a verified file too large to commit (GitHub rejects files over 100 MB), keeping its record."""
    if info["file_bytes"] > max_bytes:
        os.remove(os.path.join(target, info["file"]))
        info = dict(info, file=None, omitted=f"{info['file_bytes']} bytes on disk, over the {max_bytes} byte limit")
    return info


def save_log(base, target, compress, max_bytes):
    """Download build-log.txt, dropping a copy in the other format left by an earlier run."""
    log = keep_or_omit(download(base + "/build-log.txt", os.path.join(target, "build-log.txt"), compress),
                       target, max_bytes)
    for stale in {"build-log.txt", "build-log.txt.gz"} - {log["file"]}:
        if os.path.exists(os.path.join(target, stale)):
            os.remove(os.path.join(target, stale))
    return log


class RecheckError(Exception):
    """Comparing an intact archived build with GCS failed; its copy stays and is checked again next run."""


def go_int64(v):
    return type(v) is int and -(1 << 63) <= v < 1 << 63


# The fields TestGrid uses from started.json and finished.json, with the types its
# Go JSON decoder reads them as. A field it cannot decode (a float or string time,
# a string `passed`) fails TestGrid's read of the build, which it paints TOOL_FAIL.
STARTED_FIELDS = {"timestamp": go_int64}
FINISHED_FIELDS = {"timestamp": go_int64, "passed": lambda v: type(v) is bool, "result": lambda v: type(v) is str}


def go_decodes(doc, fields):
    """Whether TestGrid can read these fields: each absent, null or of its Go type."""
    return all(doc.get(k) is None or ok(doc[k]) for k, ok in fields.items())


def read_finished(base):
    """The fields used from the build's finished.json, None while GCS has none.
    A finished.json that is not JSON still means the build is over, and is
    malformed to TestGrid, as is one with a field TestGrid cannot decode (one that
    cannot be fetched raises); a field that is not a usable result string or Unix
    time is None."""
    try:
        finished = get_json(base + "/finished.json")
    except NotFound:
        return None
    except (BadJSON, ValueError):  # it exists, so the build is over, but it is not JSON (a failed read is raised)
        finished = None
    malformed = not isinstance(finished, dict) or not go_decodes(finished, FINISHED_FIELDS)
    finished = finished if isinstance(finished, dict) else {}
    result, stamp, passed = finished.get("result"), finished.get("timestamp"), finished.get("passed")
    return {"result": result if is_text(result, 40) else None,
            "timestamp": stamp if isinstance(stamp, (int, float)) and not isinstance(stamp, bool)
            and plausible_time(stamp, time.time()) else None,
            "stamp": stamp if go_int64(stamp) else None,  # the finish time as TestGrid reads it, plausible or not
            "passed": passed if isinstance(passed, bool) else None,
            "malformed": malformed}


def read_started(base):
    """The build's started.json as {"timestamp", "malformed"}, None if GCS has none."""
    try:
        started = get_json(base + "/started.json")
    except NotFound:
        return None
    except (BadJSON, ValueError):
        return {"timestamp": None, "malformed": True}
    stamp = started.get("timestamp") if isinstance(started, dict) else None
    return {"timestamp": stamp if isinstance(stamp, (int, float)) and not isinstance(stamp, bool)
            and plausible_time(stamp, time.time()) else None,
            "malformed": not isinstance(started, dict) or not go_decodes(started, STARTED_FIELDS)}


TESTGRID_DEADLINE_HOURS = 24  # TestGrid paints a build red once it has run this long without finishing


def red_reason(finished, started):
    if started["malformed"] or (finished and finished["malformed"]):
        return "an artifact is malformed"
    if finished is None or finished["stamp"] is None or finished["stamp"] <= 0:
        return f"it did not finish within {TESTGRID_DEADLINE_HOURS} hours"
    return f"it did not pass; Prow result {finished['result'] or 'none'}"


def painted_red(finished, started, now):
    """Whether TestGrid paints a build's Overall cell red, by its updater's own rule
    (readResult in pkg/updater/read.go; overallCell and deadline in
    pkg/updater/gcs.go): a build with started.json is red when started.json or
    finished.json is malformed (FAIL) or cannot be decoded (TOOL_FAIL), when it
    finished without passing (no `passed` field: when its result is not SUCCESS),
    or when it has gone TESTGRID_DEADLINE_HOURS without finishing.

    TestGrid also paints a build red for a malformed podinfo.json or junit file;
    those are not read here (Prow does not write them). Where a start or finish
    time is missing or bogus, TestGrid may show the build as running or unknown
    instead; this leans to red then, as archiving one build too many is better
    than missing one."""
    if started is None:
        return False  # TestGrid shows it as running ("Start timestamp for this job is 0"), never red
    if started["malformed"] or (finished is not None and finished["malformed"]):
        return True
    stamp = None if finished is None else finished["stamp"]
    if stamp is None:
        return started["timestamp"] is not None and now - started["timestamp"] > TESTGRID_DEADLINE_HOURS * 3600
    if stamp <= 0:
        return True  # not a finish to TestGrid, and its deadline (an hour after it) is long past
    return not passed_run(finished)


def passed_run(finished):
    """Whether finished.json alone says TestGrid shows the build as passed: it has a
    finish time and passed (no `passed` field: its result is SUCCESS)."""
    return (finished is not None and not finished["malformed"] and finished["stamp"] is not None
            and finished["stamp"] > 0
            and (finished["passed"] if finished["passed"] is not None else finished["result"] == "SUCCESS"))


def judge(base):
    """(finished, started, red?) for a build TestGrid no longer shows, by painted_red.
    A build that passed is not red, and its started.json is not read: TestGrid would
    still paint it red for a malformed started.json, podinfo.json or junit file, but
    Prow does not write those (cross_check reads it the same way). A file that cannot
    be fetched raises."""
    finished = read_finished(base)
    if passed_run(finished):
        return finished, None, False
    started = read_started(base)
    return finished, started, painted_red(finished, started, time.time())


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
                meta["podinfo"] = keep_or_omit(download(base + "/podinfo.json", os.path.join(target, "podinfo.json")),
                                               target, max_bytes)
                if meta["podinfo"]["md5"] != before:
                    changed.append("podinfo.json")
            elif not meta.get("log") and not meta.get("podinfo"):
                try:  # Prow writes podinfo.json up to minutes after finished.json
                    meta["podinfo"] = keep_or_omit(download(base + "/podinfo.json",
                                                            os.path.join(target, "podinfo.json")), target, max_bytes)
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


def archive(rec, target, gcs, run_id, now, max_job_hours, compress=False, max_bytes=95 << 20):
    """Archive one red build into `target` (its existing folder, or one in this run's folder)."""
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
    finished, started, red = judge(base)  # from a history gap, not decided when last looked at
    if not red:
        if finished is not None and (finished["stamp"] is not None or started is None):
            return "not-red", rec  # finished, or finished without ever starting (TestGrid never paints that red)
        return ("watching" if now - rec["created"] <= GIVE_UP_DAYS * 86400 else "stopped-watching"), rec
    rec = {k: v for k, v in rec.items() if k != "watch"}
    rec["backfilled"] = (f"TestGrid no longer showed this build (a gap between runs); by TestGrid's rule it "
                         f"was red ({red_reason(finished, started)})")
    try:
        return store(rec, target, existed, old, finished, base, run_id, now, max_job_hours, compress, max_bytes)
    except Exception as e:
        e.red_rec = rec  # now known to be red: retried and given up on like any other red build
        raise


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
                podinfo = keep_or_omit(download(base + "/podinfo.json", os.path.join(target, "podinfo.json")),
                                       target, max_bytes)
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
    """Remove build folders a killed run left without a meta.json, and run folders
    left empty (only call while holding the lock).

    Only files a download writes are removed; a folder holding anything else is reported, not touched.
    """
    for run_rel in run_dirs(root):
        run_abs = os.path.join(root, run_rel)
        for job in os.listdir(run_abs):
            job_abs = os.path.join(run_abs, job)
            if job in RUN_FILES or not real_dir(job_abs):
                continue
            for build in os.listdir(job_abs):
                target = os.path.join(job_abs, build)
                if not real_dir(target) or os.path.exists(os.path.join(target, "meta.json")):
                    continue
                names = os.listdir(target)
                if all(n in LOG_FILES or (n.startswith(".") and n.endswith(".part")) for n in names):
                    for n in names:
                        os.remove(os.path.join(target, n))
                    remove_build_dir(target)
                else:
                    warnings.append(f"{run_rel}/{job}/{build}: no meta.json but unexpected files {sorted(names)[:5]}")
            if not os.listdir(job_abs):
                remove_if_empty(job_abs)
        # a run killed before it archived or reported anything leaves an empty folder
        if not os.listdir(run_abs):
            remove_if_empty(run_abs)
            remove_if_empty(os.path.dirname(run_abs))


def index_entry(job, build, meta, now, rel=None):
    t = meta["started"] or meta["created"]
    if not plausible_time(t, now):
        raise ValueError(f"implausible time {t!r}")
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
        # specific failing tests first, then job-level rows such as .Pod, .Overall last
        "red_tests": sorted({c["test"] for t in meta["tabs"] for c in t["red_cells"]},
                            key=lambda name: (name == f"{job}.Overall", name.startswith(f"{job}."), name)),
        "dir": rel,
        "run": rel.split("/")[2] if rel else None,
        "files": [f"{rel}/{name}" for name, info in files if info and not info.get("omitted")] if rel else [],
        "remote": remote,
        "bytes": sum(info["bytes"] for _, info in files if info),
        "settled": meta.get("settled") if isinstance(meta.get("settled"), bool) else None,
        "final": meta.get("final") if isinstance(meta.get("final"), bool) else None,
        "backfilled": bool(meta.get("backfilled")),
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
        tests = e["red_tests"]
        shown = (md_cell(tests[0][:90]) + (f" (+{len(tests) - 1})" if len(tests) > 1 else "")) if tests else ""
        if e.get("backfilled") and not tests:
            shown = "(taken from GCS: red by TestGrid's rule, but TestGrid no longer showed it)"
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
    lines += build_rows(recent) + ["", "## Every week", ""]
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
                kept[k] = v
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
            for name in ("current_job_hours", "job_hours"):  # checked again where they are used
                if name in v:
                    tabs[k][name] = v[name]
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


def recheck_rec(meta, now):
    """The build record of an archived meta.json, so it can be re-checked after TestGrid drops it."""
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


def backfill_gaps(gaps, gcs, pool, deadline, visible, skip):
    """For each tab with a history gap, the builds GCS has from that time that TestGrid
    painted red but no longer shows: {tab key: [build records] or the Exception that
    stopped it}, plus "watch": builds there still running, to judge once they finish."""
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
        candidates += [(key, r, b) for b in fut.result()
                       if b not in visible.get(r["query"], ()) and f"{job}/{b}" not in skip]
    looked_up = [pool.submit(judge, f"{gcs}/{r['query']}/{b}") for _, r, b in candidates]
    cf.wait(looked_up, timeout=max(0, deadline - time.monotonic()))
    recs, watch = {}, {}
    for (key, r, b), fut in zip(candidates, looked_up, strict=True):
        if isinstance(out[key], Exception):
            continue
        if not fut.done():
            out[key] = TimeoutError("run timeout reached")
            continue
        job = r["query"].rsplit("/", 1)[-1]
        sighting = {"dashboard": r["dashboard"], "tab": r["tab"], "tab_status": r.get("status"), "red_cells": []}
        fin, started, is_red = fut.result() if fut.exception() is None else (None, None, None)
        if is_red is None or (not is_red and started is not None and (fin is None or fin["stamp"] is None)
                              and not (fin and fin["malformed"])):
            # Its files could not be read, or it is still running: watched, so it is looked at
            # again (first in this run) until it is decided, without holding back the rest of the gap.
            watch.setdefault(f"{job}/{b}", {"job": job, "build": b, "query": r["query"], "started": None,
                                            "created": created(b), "tabs": [], "watch": True})["tabs"].append(sighting)
            continue
        if is_red:
            rec = recs.get(f"{job}/{b}")
            if rec is None:
                recs[f"{job}/{b}"] = rec = {
                    "job": job, "build": b, "query": r["query"], "started": None, "created": created(b), "tabs": [],
                    "backfilled": f"TestGrid no longer showed this build (a gap between runs); by TestGrid's rule it "
                                  f"was red ({red_reason(fin, started)})"}
            rec["tabs"].append(sighting)
            # listed under each tab whose gap holds it, so it is kept if another tab's search fails
            # (run() takes each build once)
            out[key].append(rec)
    for k in [k for k in watch if k in recs]:  # unreadable in one tab's look, red in another's: red
        recs[k]["tabs"] += [t for t in watch.pop(k)["tabs"] if t not in recs[k]["tabs"]]
    out["watch"] = list(watch.values())
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
                   help="verify but do not store files larger than this on disk (GitHub rejects files over 100 MB)")
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
    pool = cf.ThreadPoolExecutor(ctx.limit.ceiling, initializer=bind_context, initargs=(ctx,))
    # The GitHub runner ends a timed-out or cancelled step with SIGINT, then SIGTERM;
    # handle both the same way.
    previous_term = signal.signal(signal.SIGTERM, interrupt)
    try:
        return run(args, root, run_start, pool, deadline)
    except KeyboardInterrupt:  # outside the download wait (see run()): nothing to record yet
        ctx.abandoned.set()  # workers stop at their next read and cannot write
        print("ERROR interrupted; unfinished builds are picked up by the next run", file=sys.stderr)
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        # Workers still blocked past the deadline are abandoned: they cannot write
        # any more (see replace()), and the process exit does not wait for them.
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
    reports, builds, visible = scan(args.testgrid, args.gcs, dashboards, pool, deadline)
    job_hours = {}  # per tab, the job durations to remember for the next run
    for r in reports:
        # A job known to run longer than --max-job-hours gets its own margin (see job_margin).
        if r.get("tab") is not None and not r.get("error"):
            key = f"{r['dashboard']}#{r['tab']}"
            r["max_job_hours"], job_hours[key] = job_margin(state["tabs"].get(key) or {}, r.get("max_job_hours"),
                                                            run_start, args.max_job_hours)

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
            floor = max(prev["oldest"] or -math.inf, prev["scan"] - r["max_job_hours"] * 3600)
            if r["oldest"] > floor:
                if floor < horizon and not first:
                    warnings.append(f"{key}: the gap in TestGrid's history reaches back to {iso(floor)}; red builds "
                                    f"before {iso(horizon)} may be missing, as GCS is searched {GIVE_UP_DAYS} days back")
                gaps.append((key, r, max(floor, horizon)))
        if r.get("oldest"):
            read_ok[key] = {"scan": run_start, "oldest": r["oldest"], **job_hours.get(key, {})}
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
        selected = [b for b in selected if (b["started"] or b["created"]) >= cutoff]
    chosen = {f"{b['job']}/{b['build']}" for b in selected}
    retry = [u["rec"] for k, u in state["unresolved"].items() if k not in chosen and k not in given_up]
    # builds a history gap showed still running: judged once they finish
    retry += [u["rec"] for k, u in state["watch"].items()
              if k not in chosen and k not in given_up and k not in state["unresolved"] and k not in known]
    # Archived builds that are not final yet are compared with GCS again, even
    # after TestGrid drops them: their logs may still be replaced by Prow.
    queued = chosen | set(state["unresolved"]) | set(given_up) | set(state["watch"])
    rechecks = 0
    for key, rel in sorted(known.items()):
        if key in queued:
            continue
        try:
            meta = read_json(os.path.join(root, rel, "meta.json"))
        except (ValueError, OSError):
            continue  # reported as unreadable by the index
        if valid_meta(meta) and not meta.get("final") and (rec := recheck_rec(meta, run_start)):
            retry.append(rec)
            rechecks += 1
    # A gap in a tab's history: list the job's builds from that time in GCS and
    # archive the ones TestGrid painted red by its own rule, since it no longer shows them.
    backfill_errors, backfilled, new_watch, taken = [], 0, {}, set()

    def last_scan(key):
        return (state["tabs"].get(key) or {"scan": archive_start})["scan"]
    if gaps and not args.list_only:
        backfill = backfill_gaps(gaps, args.gcs, pool, deadline, visible, set(known) | queued)
        new_watch = {f"{w['job']}/{w['build']}": w for w in backfill.pop("watch", [])}
        retry += list(new_watch.values())
        for key, r, floor in gaps:
            found = backfill.get(key)
            if isinstance(found, Exception):
                backfill_errors.append({"build": None, "error": f"{key}: listing GCS for the history gap failed: "
                                                               f"{type(found).__name__}: {found}"})
                # Keep the old scan time (none for a tab not scanned before), so the next run
                # searches this gap again; it never searches more than GIVE_UP_DAYS back.
                if key in state["tabs"]:
                    old = state["tabs"][key]
                    tab_scans[key] = dict(job_hours.get(key, {}), scan=old["scan"], oldest=old["oldest"])
                else:
                    tab_scans.pop(key, None)
                outcome = "looking them up in GCS failed; the next run tries again"
            else:
                new = [rec for rec in found if f"{rec['job']}/{rec['build']}" not in taken]  # also in another tab's gap
                taken.update(f"{rec['job']}/{rec['build']}" for rec in new)
                retry += new
                backfilled += len(new)
                running = sum(1 for w in new_watch.values() if any(f"{t['dashboard']}#{t['tab']}" == key for t in w["tabs"]))
                warnings.append(f"{key}: TestGrid only shows builds since {iso(r['oldest'])}, but the last scan "
                                f"({iso(last_scan(key))}) needed them from {iso(floor)}; GCS was "
                                f"searched instead, and {len(found)} red builds from that time were taken from it"
                                + (f" ({running} not decided yet, still running or unreadable, are watched)"
                                   if running else ""))
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
    errors += [{"build": None, "error": f"duplicate copies: {d}"} for d in duplicates]
    max_bytes = int(args.max_file_mb * (1 << 20))

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
                               max_bytes): b
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
        ctx.abandoned.set()
    for fut in futures:
        b = futures[fut]
        key = f"{b['job']}/{b['build']}"
        previous = state["unresolved"].get(key, {})
        try:
            if fut in not_done:
                if archived_intact(target_of(b), deep=True):
                    # Finished just as the run gave up, or already on disk; one
                    # that is not final yet is compared with GCS next run.
                    outcomes.setdefault("already-archived", []).append(key)
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
                    message = f"no longer watched after {GIVE_UP_DAYS} days, never found red: {message}"
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
    # Job folders a failed or pending download left empty (workers leave them, see remove_build_dir)
    run_abs = os.path.join(root, run_rel)
    for job in listdir(run_abs):
        if job not in RUN_FILES and real_dir(os.path.join(run_abs, job)) and not listdir(os.path.join(run_abs, job)):
            remove_if_empty(os.path.join(run_abs, job))
    warnings += [f"{key}: still not finished {GIVE_UP_DAYS} days after it was made, and TestGrid never paints such "
                 "a build red (it has no usable start time); no longer watched"
                 for key in outcomes.get("stopped-watching", [])]

    # A possible gap in TestGrid history, a tab gone from its dashboard or a lost
    # retry list means builds may be missing without an error naming them.
    serious = [w for w in warnings if any(k in w for k in SERIOUS)]
    report_ = {"run": run_id, "dir": run_rel, "started": run_start, "since_hours": args.since_hours,
               "max_job_hours": args.max_job_hours, "tabs": reports, "red_builds_seen": len(builds),
               "retried_from_state": len(retry) - rechecks - backfilled - len(new_watch), "rechecked": rechecks,
               "backfilled_from_gcs": backfilled, "outcomes": outcomes,
               "errors": errors, "warnings": warnings, "serious_warnings": serious, "interrupted": interrupted}
    run_json = os.path.join(root, run_rel, "run.json")
    # Save what the next run needs before drawing the pages and charts: a failure
    # there must not lose a build that is about to scroll off TestGrid.
    write_json(os.path.join(root, "state.json"),
               {"tabs": tab_scans, "unresolved": unresolved, "given_up": given_up, "watch": watch})
    write_json(run_json, dict(report_, errors=errors + [{"build": None, "error": "the run stopped before it finished"}]))
    missing = ([{"rec": u["rec"], "status": u["last_error"] or "waiting for GCS upload"} for u in unresolved.values()]
               + [{"rec": u["rec"], "status": f"given up: {u['last_error']}"} for u in given_up.values()])
    entries = None
    try:
        entries, _ = write_index(root, time.time(), report_, missing)  # adds unreadable files to errors
    except Exception as e:
        errors.append({"build": None, "error": f"writing the index or charts failed: {type(e).__name__}: {e}"})
    elapsed = time.time() - run_start
    limit = ctx.limit
    concurrency = {"cpus": cpu_count(), "start": args.start_concurrency, "ceiling": limit.ceiling,
                   "peak": limit.peak, "final": int(limit.cap), "pushbacks": limit.pushbacks}
    final = dict(report_, errors=errors, finished=time.time(), bytes_downloaded=downloaded, concurrency=concurrency)
    write_json(run_json, final)
    if entries is not None:
        write_run_readme(root, run_rel, final, entries)

    counts = [f"{k}={len(v)}" for k, v in sorted(outcomes.items())] + [f"errors={len(errors)}"]
    print(f"run {run_id}: {len(reports)} tabs read ({len(tab_errors)} failed), {len(builds)} red builds on TestGrid, "
          f"{len(selected)} selected, {len(retry) - rechecks - backfilled - len(new_watch)} retried, "
          f"{rechecks} re-compared with GCS, "
          f"{backfilled} taken from GCS for history gaps: "
          f"{', '.join(counts)}; archive holds {len(entries) if entries is not None else '?'} builds")
    print(f"  {downloaded / 1e6:.1f} MB in {elapsed:.1f}s ({downloaded / 1e6 / max(elapsed, 1e-9):.1f} MB/s); "
          f"in-flight requests: start {concurrency['start']}, peak {concurrency['peak']}, "
          f"final {concurrency['final']} of max {concurrency['ceiling']}, {concurrency['pushbacks']} pushbacks")
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
