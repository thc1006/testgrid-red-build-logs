#!/usr/bin/env python3
"""Archive the raw Prow build-log.txt behind every red cell on TestGrid dashboards.

A cell counts as red exactly when TestGrid's own UI paints it red (#a00):
FAIL, TIMED_OUT, CATEGORIZED_FAIL and TOOL_FAIL. Tables are read the way the
dashboard page loads them (tab base options applied, no extra row filter), so
the red cells are the ones you see when you open the tab. Each column is one
Prow build: the archive keeps one log per build and skips builds it already
holds, so every run picks up whatever turned red since the previous run and
the first run backfills everything TestGrid still shows. Builds that could not
be archived yet are remembered in state.json and retried straight from GCS,
even after they scroll off TestGrid.

Layout under --archive:
  logs/<job>/<build>/build-log.txt  raw log, length and md5 checked against GCS
                                    (build-log.txt.gz with --gzip; meta.json keeps the raw md5)
  logs/<job>/<build>/podinfo.json   only when the build has no log (pod never ran)
  logs/<job>/<build>/meta.json      tabs, red cells, TestGrid messages, result
  runs/<UTC time>.json              what one run scanned, archived and failed
  charts/*.svg                      trend charts shown in INDEX.md
  INDEX.md                          charts, builds still missing, the last 14 days of builds
  weeks/<ISO week>.md, .json        every archived build, one page per ISO week (UTC)
  index.json                        the list of weekly pages
  state.json                        per-tab last scan, builds to retry or given up on
"""
import argparse
import base64
import concurrent.futures as cf
import contextlib
import datetime as dt
import fcntl
import glob
import gzip
import hashlib
import http.client
import json
import math
import os
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

import report

TESTGRID = "https://testgrid.k8s.io"
GCS = "https://storage.googleapis.com"
DASHBOARDS = ["sig-release-master-blocking", "sig-release-master-informing"]
# TestGrid TestStatus values its UI paints red (#a00). BUILD_FAIL (11) is
# painted black and UNKNOWN (6) grey, so those cells are not red.
RED = {9: "TIMED_OUT", 10: "CATEGORIZED_FAIL", 12: "FAIL", 14: "TOOL_FAIL"}
NO_RESULT = 0
# Prow build IDs are snowflakes: (id >> 22) + this epoch is the creation time in ms.
PROW_EPOCH_MS = 1288834974657
JOB_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
BUILD_RE = re.compile(r"[0-9]{1,20}")
LOG_FILES = {"build-log.txt", "build-log.txt.gz", "podinfo.json"}
RETRIES = 4
BACKOFF = 2.0  # seconds before the first retry, doubled after each attempt
# A response body must finish within BASE_DEADLINE plus its size at MIN_RATE,
# so a server that trickles bytes cannot stall a request (socket timeouts are
# per read); the whole run is bounded by --run-timeout-minutes on top of that.
BASE_DEADLINE = 120.0
MIN_RATE = 50_000  # bytes per second
GIVE_UP_DAYS = 14  # stop retrying a build GCS still lacks after this long
UNLISTED_DAYS = 30  # forget a tab missing from its dashboard's summary after this long
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
    """What one run's threads share: its request limiter and its abandon flag.

    main() binds a fresh context to the main thread and to every worker of its
    pool, so a later run in the same process never re-arms or shares an earlier
    run's abandoned workers.
    """

    def __init__(self, limit):
        self.limit = limit
        self.abandoned = threading.Event()


_local = threading.local()
_DEFAULT_CONTEXT = RunContext(AdaptiveLimit(8, 8))


def context():
    return getattr(_local, "context", _DEFAULT_CONTEXT)


def bind_context(ctx):
    _local.context = ctx


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


def read_body(r):
    body = b"".join(chunks(r, r.url))
    size = r.headers.get("Content-Length")
    # http.client returns short reads instead of raising when a body is cut off.
    if size is not None and size.isdigit() and int(size) != len(body):
        raise IntegrityError(f"{r.url}: got {len(body)} of {size} bytes")
    return body


def read_json_body(r):
    body = read_body(r)
    try:
        return json.loads(body)
    except ValueError as e:  # a body cut short without a length: retry
        raise IntegrityError(f"{r.url}: bad JSON: {e}") from None


def request(url, consume=read_body, headers=None, timeout=120, method="GET"):
    """Fetch url with retries; consume(response) reads the body."""
    ctx = context()
    for attempt in range(RETRIES):
        if ctx.abandoned.is_set():
            raise Abandoned(url)
        ctx.limit.acquire()
        pushback = False
        try:
            req = urllib.request.Request(url, headers=headers or {}, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return consume(r)
        except urllib.error.HTTPError as e:
            e.close()
            if e.code == 404:
                raise NotFound(url) from None
            pushback = e.code in (429, 503, 504)
            if (e.code < 500 and e.code not in (408, 429)) or attempt == RETRIES - 1:
                raise
        except IntegrityError:
            if attempt == RETRIES - 1:
                raise
        except TRANSIENT:
            pushback = True
            if attempt == RETRIES - 1:
                raise
        finally:
            ctx.limit.release(pushback)
        time.sleep(BACKOFF * 2 ** attempt)


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


def red_columns(table):
    """Map column index -> red cells in that column (one column is one build).

    Rows are run-length encoded. `messages` only has entries for cells with a
    result, so a cell's message index skips the NO_RESULT cells before it.
    """
    width = len(table["changelists"])
    if len(table["timestamps"]) != width:
        raise ValueError(f"{width} changelists but {len(table['timestamps'])} timestamps")
    reds = {}
    for test in table.get("tests", []):
        runs = test["statuses"]
        if sum(r["count"] for r in runs) != width:
            raise ValueError(f"row {test['name']!r} does not span {width} columns")
        messages = test.get("messages") or []
        aligned = len(messages) == sum(r["count"] for r in runs if r["value"] != NO_RESULT)
        col = idx = 0
        for r in runs:
            value, count = r["value"], r["count"]
            if value in RED:
                for k in range(count):
                    reds.setdefault(col + k, []).append({
                        "test": test["name"],
                        "status": RED[value],
                        "message": messages[idx + k] if aligned else None,
                    })
            col += count
            if value != NO_RESULT:
                idx += count
    return reds


def read_tab(table, now):
    """Validate one tab's table; return (query, job, oldest start, [(build, start, cells)])."""
    reds = red_columns(table)
    query = table["query"]
    if not isinstance(query, str) or "," in query or query.count("/") < 1:
        raise ValueError(f"unsupported GCS query {query!r}")
    job = query.rsplit("/", 1)[-1]
    if not JOB_RE.fullmatch(job):
        raise ValueError(f"unexpected job name {job!r}")
    starts = []
    for stamp in table["timestamps"]:
        if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
            raise ValueError(f"implausible timestamp {stamp!r}")
        if stamp == 0:
            starts.append(None)  # TestGrid's marker for a column without a start time
        elif plausible_time(stamp / 1000, now):
            starts.append(int(stamp) // 1000)
        else:
            raise ValueError(f"implausible timestamp {stamp!r}")
    found = []
    for col, cells in sorted(reds.items()):
        build = table["changelists"][col]
        if not isinstance(build, str) or not BUILD_RE.fullmatch(build) or not plausible_time(created(build), now):
            raise ValueError(f"unexpected build id {build!r}")
        found.append((build, starts[col], cells))
    known = [s for s in starts if s is not None]
    return query, job, (min(known) if known else None), found


def scan(testgrid, dashboards, pool, deadline):
    """Read every tab of every dashboard; return (tab reports, red builds by (job, build))."""
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
            status = info.get("overall_status") if isinstance(info, dict) else None
            tabs.append((dashboard, tab, status))

    futures = [pool.submit(get_json, table_url(testgrid, d, t)) for d, t, _ in tabs]
    cf.wait(futures, timeout=max(0, deadline - time.monotonic()))
    if not all(f.done() for f in futures):
        abandoned.set()
    builds = {}
    for (dashboard, tab, status), fut in zip(tabs, futures, strict=True):
        report_ = {"dashboard": dashboard, "tab": tab, "status": status}
        reports.append(report_)
        try:
            if not fut.done():
                raise TimeoutError("run timeout reached before the table loaded")
            query, job, oldest, found = read_tab(fut.result(), now)
            for build, _, _ in found:
                other = builds.get((job, build))
                if other and other["query"] != query:
                    raise ValueError(f"{job}/{build} is also listed under {other['query']}")
        except Exception as e:
            report_["error"] = f"{type(e).__name__}: {e}"
            continue
        report_.update(query=query, columns=len(fut.result()["changelists"]), oldest=oldest, red_builds=len(found))
        for build, started, cells in found:
            rec = builds.setdefault((job, build), {"job": job, "build": build, "query": query,
                                                   "started": started, "created": created(build), "tabs": []})
            rec["tabs"].append({"dashboard": dashboard, "tab": tab, "tab_status": status, "red_cells": cells})
    return reports, builds


def gcs_md5(headers):
    for value in headers.get_all("x-goog-hash") or []:
        for part in value.split(","):
            k, _, v = part.strip().partition("=")
            if k == "md5":
                return base64.b64decode(v).hex()
    return None


class Gunzip:
    """Streaming gunzip: handles concatenated members and trailing zero padding, like gzip(1)."""

    def __init__(self):
        self._padding = False
        self._new()

    def _new(self):
        self._d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._open = False  # bytes of the current member have been fed

    def feed(self, data):
        out = []
        try:
            while data:
                if not self._open and not data.strip(b"\0"):
                    self._padding = True  # zeros after the last member
                    break
                if self._padding:
                    raise IntegrityError("data after gzip padding")
                out.append(self._d.decompress(data))
                self._open = True
                if self._d.eof:
                    data = self._d.unused_data
                    self._new()
                else:
                    data = b""
        except zlib.error as e:
            raise IntegrityError(f"bad gzip data: {e}") from None
        return b"".join(out)

    def finish(self):
        out = self._d.flush()
        if self._open and not self._d.eof:
            raise IntegrityError("truncated gzip stream")
        return out


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
        stored_enc = r.headers.get("x-goog-stored-content-encoding", "identity")
        sent_enc = r.headers.get("Content-Encoding", "identity")
        if sent_enc not in ("identity", "gzip"):
            raise IntegrityError(f"{url}: unsupported Content-Encoding {sent_enc!r}")
        stored_len = r.headers.get("x-goog-stored-content-length")
        stored_md5 = gcs_md5(r.headers)
        sent_len = r.headers.get("Content-Length")
        raw, raw_len, out, out_len = hashlib.md5(), 0, hashlib.md5(), 0
        gunzip = Gunzip() if sent_enc == "gzip" else None
        try:
            f = open(part, "wb")
        except OSError as e:
            raise LocalIOError(f"{part}: {e}") from None
        with f, (
                # a fixed name and mtime keep the .gz bytes identical for the same log
                gzip.GzipFile(filename=os.path.basename(final), fileobj=f, mode="wb", compresslevel=6, mtime=0)
                if compress
                else contextlib.nullcontext(f)) as sink:

            def write(data):
                nonlocal out_len
                try:
                    sink.write(data)
                except OSError as e:
                    raise LocalIOError(f"{part}: {e}") from None
                out.update(data)
                out_len += len(data)
            for chunk in chunks(r, url):
                raw.update(chunk)
                raw_len += len(chunk)
                write(gunzip.feed(chunk) if gunzip else chunk)
            if gunzip:
                write(gunzip.finish())
        if sent_len is not None and int(sent_len) != raw_len:
            raise IntegrityError(f"{url}: got {raw_len} of {sent_len} bytes")
        # GCS's length and md5 describe the bytes in their stored encoding.
        if sent_enc == stored_enc:
            got_md5, got_len = raw.hexdigest(), raw_len
        elif stored_enc == "identity":
            got_md5, got_len = out.hexdigest(), out_len
        else:
            raise IntegrityError(f"{url}: served as {sent_enc!r} but stored as {stored_enc!r}")
        if stored_len is not None and int(stored_len) != got_len:
            raise IntegrityError(f"{url}: got {got_len} bytes, GCS stores {stored_len}")
        if stored_md5 is not None and stored_md5 != got_md5:
            raise IntegrityError(f"{url}: got md5 {got_md5}, GCS stores {stored_md5}")
        replace(part, final)
        return {"bytes": out_len, "md5": out.hexdigest(),
                "verified": stored_len is not None and stored_md5 is not None,
                "file": os.path.basename(final), "file_bytes": os.path.getsize(final)}

    try:
        # Accept gzip so a gzip-stored object arrives exactly as stored and stays verifiable.
        return request(url, consume, headers={"Accept-Encoding": "gzip"})
    finally:
        if os.path.exists(part):
            os.remove(part)


def write_json(path, data):
    write_file(path, json.dumps(data, indent=2, ensure_ascii=False) + "\n")


def write_file(path, text):
    tmp = temp_path(path)
    try:
        with open(tmp, "w") as f:
            f.write(text)
        replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def read_json(path):
    """Parsed JSON, None if the file is missing; raises ValueError if it is corrupt."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def remove_if_empty(path):
    try:
        os.rmdir(path)
    except OSError:
        pass


def file_md5(path):
    md5 = hashlib.md5()
    with (gzip.open if path.endswith(".gz") else open)(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            md5.update(chunk)
    return md5.hexdigest()


def intact(target, meta, deep=False):
    """Whether the files meta.json describes are on disk with the recorded size (and md5 if deep)."""
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
                if deep and file_md5(path) != info["md5"]:
                    return False
            except (OSError, EOFError, zlib.error, KeyError, TypeError, AttributeError):
                return False
    return True


def valid_meta(meta):
    return (isinstance(meta, dict) and isinstance(meta.get("tabs"), list)
            and all(isinstance(t, dict) and "dashboard" in t and "tab" in t for t in meta["tabs"]))


def merge_tabs(old, new):
    """Old tab sightings plus any tab this build is newly red on (first sighting wins)."""
    seen = {(t["dashboard"], t["tab"]) for t in old}
    added = [t for t in new if (t["dashboard"], t["tab"]) not in seen]
    return old + added, bool(added)


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


def archive(rec, root, gcs, run_id, now, max_job_hours, compress=False, max_bytes=95 << 20):
    target = os.path.join(root, "logs", rec["job"], rec["build"])
    meta_path = os.path.join(target, "meta.json")
    base = f"{gcs}/{rec['query']}/{rec['build']}"
    try:
        old = read_json(meta_path)
    except ValueError:
        old = None  # corrupt meta.json: archive the build again
    old = old if valid_meta(old) else None
    if old and intact(target, old, deep=True):
        tabs, grew = merge_tabs(old["tabs"], rec["tabs"])
        meta = dict(old, tabs=tabs)
        outcome = "already-archived"
        if not old.get("log"):
            # Archived without a log: check whether it has appeared since.
            try:
                meta["log"] = save_log(base, target, compress, max_bytes)
                meta["log_recovered_by_run"] = run_id
                outcome = "log-recovered"
            except NotFound:
                pass
        if grew or outcome != "already-archived":
            write_json(meta_path, meta)
        return outcome, meta
    try:
        finished = get_json(base + "/finished.json")
    except NotFound:
        finished = None
    os.makedirs(target, exist_ok=True)
    podinfo = None
    try:
        try:
            log = save_log(base, target, compress, max_bytes)
        except NotFound:
            if old and old.get("log"):
                raise RuntimeError(f"the archived log is damaged and GCS no longer has it ({base})") from None
            if finished is None:
                remove_if_empty(target)
                age = now - (rec["started"] or rec["created"])
                if age > 2 * max_job_hours * 3600:
                    raise RuntimeError(f"red on TestGrid but nothing in GCS after {age / 3600:.0f}h "
                                       f"(gs://{rec['query']}/{rec['build']})") from None
                return "pending", rec  # still uploading: look again next run
            # The build finished without a log because its pod never ran; keep
            # Prow's pod status instead.
            log = None
            try:
                podinfo = keep_or_omit(download(base + "/podinfo.json", os.path.join(target, "podinfo.json")),
                                       target, max_bytes)
            except NotFound:
                pass
    except Exception:
        remove_if_empty(target)
        raise
    tabs = merge_tabs(old["tabs"], rec["tabs"])[0] if old else rec["tabs"]
    meta = dict(rec,
                tabs=tabs,
                result=(finished or {}).get("result"),
                finished=(finished or {}).get("timestamp"),
                prow_url=f"https://prow.k8s.io/view/gs/{rec['query']}/{rec['build']}",
                log_url=base + "/build-log.txt",
                log=log,
                podinfo=podinfo,
                archived_by_run=run_id)
    write_json(meta_path, meta)
    return ("archived" if log else "archived-without-log") if old is None else "repaired", meta


def index_entry(job, build, meta, now):
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
        "result": meta.get("result"),
        "tabs": [f"{t['dashboard']}#{t['tab']}" for t in meta["tabs"]],
        # specific failing tests first, then job-level rows such as .Pod, .Overall last
        "red_tests": sorted({c["test"] for t in meta["tabs"] for c in t["red_cells"]},
                            key=lambda name: (name == f"{job}.Overall", name.startswith(f"{job}."), name)),
        "files": [f"logs/{job}/{build}/{name}" for name, info in files if info and not info.get("omitted")],
        "remote": remote,
        "bytes": sum(info["bytes"] for _, info in files if info),
        "prow_url": meta.get("prow_url") or f"https://prow.k8s.io/view/gs/{meta['query']}/{build}",
    }


def load_index(root, now):
    """Every readable meta.json as an index entry, plus a list of unreadable ones."""
    entries, bad = [], []
    logs = os.path.join(root, "logs")
    for job in sorted(os.listdir(logs)) if os.path.isdir(logs) else []:
        if not os.path.isdir(os.path.join(logs, job)):
            continue
        for build in sorted(os.listdir(os.path.join(logs, job))):
            meta_path = os.path.join(logs, job, build, "meta.json")
            if not os.path.isfile(meta_path):
                continue
            try:
                meta = read_json(meta_path)
                if not valid_meta(meta):
                    raise ValueError("not an archive meta.json")
                entries.append(index_entry(job, build, meta, now))
            except (ValueError, KeyError, TypeError, AttributeError, OverflowError, OSError) as e:
                bad.append(f"logs/{job}/{build}/meta.json: {type(e).__name__}: {e}")
    entries.sort(key=lambda e: (e["time"], e["build"]), reverse=True)
    return entries, bad


def md_cell(s):
    return str(s).replace("|", "\\|").replace("\n", " ")


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
        if files:
            links = [f"[{p.rsplit('/', 1)[1]}]({prefix}{p})" for p in e["files"]]
            links += [f"[{name} in GCS (too large to store)]({url})" for name, url in e.get("remote") or []]
            last = " ".join(links) or "none"
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
        with open(path) as f:
            if f.read() == text:
                return
    except (FileNotFoundError, UnicodeDecodeError):
        pass
    write_file(path, text)


def write_index(root, now, current_run=None, missing=()):
    """Rewrite INDEX.md, the weekly pages and the charts. `missing` are red builds not archived (yet)."""
    entries, bad = load_index(root, now)
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
             f"{' and '.join(DASHBOARDS)}; times are UTC.", ""]
    lines += report.write_charts(root, counted, now, current_run)
    if waiting:
        lines += ["## Not archived yet", "", f"{len(waiting)} red builds without a verified local copy yet."]
        lines += build_rows(waiting, files=False) + [""]
    lines += [f"## Builds archived in the last {report.HEAT_DAYS} days", "",
              f"{len(recent)} builds since {first_day.isoformat()} (UTC), newest first."]
    lines += build_rows(recent) + ["", "## Every week", ""]
    lines += [f"- [{w['week']}]({w['page']}): {short(dt.date.fromisoformat(w['from']))} – "
              f"{short(dt.date.fromisoformat(w['to']))}, {w['builds']} builds" for w in listing]
    write_text(os.path.join(root, "INDEX.md"), "\n".join(lines) + "\n")
    return len(entries), bad


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
        state = read_json(os.path.join(root, "state.json")) or {}
    except ValueError as e:
        warnings.append(f"state.json is corrupt ({e}); starting without it")
        state = {}
    if not isinstance(state, dict):
        state = {}

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
    return {"tabs": tabs, "unresolved": entries("unresolved"), "given_up": entries("given_up"),
            "no_log": entries("no_log")}


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
    """Remove temp files a killed run left behind (only call while holding the lock)."""
    removed, base = 0, glob.escape(root)
    for path in {*glob.glob(os.path.join(base, "**", ".*.part"), recursive=True),
                 *glob.glob(os.path.join(base, "charts", "*.part"))}:
        with contextlib.suppress(OSError):
            os.remove(path)
            removed += 1
    return removed


def archived_intact(root, job, build, deep=False):
    target = os.path.join(root, "logs", job, build)
    try:
        meta = read_json(os.path.join(target, "meta.json"))
    except ValueError:
        return False
    return valid_meta(meta) and intact(target, meta, deep)


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
    run_id = dt.datetime.fromtimestamp(run_start, dt.UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    root = args.archive
    lock = None
    if not args.list_only:
        lock = lock_archive(root)
        if lock is None:
            print(f"ERROR another run holds {os.path.join(root, '.lock')}", file=sys.stderr)
            return 2
    ctx = RunContext(AdaptiveLimit(args.start_concurrency, args.max_concurrency))
    bind_context(ctx)
    pool = cf.ThreadPoolExecutor(ctx.limit.ceiling, initializer=bind_context, initargs=(ctx,))
    deadline = time.monotonic() + args.run_timeout_minutes * 60
    try:
        return run(args, root, run_start, run_id, pool, deadline)
    except KeyboardInterrupt:  # SIGINT, e.g. a GitHub step timeout or cancel
        ctx.abandoned.set()  # workers stop at their next read and cannot write
        print("ERROR interrupted; unfinished builds are picked up by the next run", file=sys.stderr)
        return 130
    finally:
        # Workers still blocked past the deadline are abandoned: they cannot write
        # any more (see replace()), and the process exit does not wait for them.
        pool.shutdown(wait=not ctx.abandoned.is_set(), cancel_futures=True)
        if lock:
            lock.close()


def run(args, root, run_start, run_id, pool, deadline):
    ctx = context()
    warnings = []
    if not args.list_only:
        sweep_parts(root)
    state = load_state(root, warnings)
    dashboards = args.dashboards or DASHBOARDS
    reports, builds = scan(args.testgrid, dashboards, pool, deadline)
    margin = args.max_job_hours * 3600

    read_ok = {}
    for r in reports:
        if r.get("tab") is None or r.get("error"):
            continue
        key = f"{r['dashboard']}#{r['tab']}"
        prev = state["tabs"].get(key)
        # Builds the last scan saw that might still have been running started after
        # max(that scan's oldest column, its time - max_job_hours); if the oldest
        # column now is later than that, some of them may have scrolled off unseen.
        if prev and r.get("oldest"):
            floor = max(prev["oldest"] or -math.inf, prev["scan"] - margin)
            if r["oldest"] > floor:
                warnings.append(f"{key}: TestGrid only shows builds since {iso(r['oldest'])}, but the last scan "
                                f"({iso(prev['scan'])}) needed them from {iso(floor)}; red builds in between may be missing")
        if r.get("oldest"):
            read_ok[key] = {"scan": run_start, "oldest": r["oldest"]}
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

    # A given-up build whose files are back on disk (restored, or archived late) is done.
    given_up = {k: v for k, v in state["given_up"].items()
                if not archived_intact(root, *k.split("/", 1), deep=True)}
    selected = [b for b in builds.values() if f"{b['job']}/{b['build']}" not in given_up]
    if args.since_hours is not None:
        cutoff = run_start - args.since_hours * 3600
        if state["tabs"]:
            # never skip builds that finished since an earlier scan saw them running
            cutoff = min(cutoff, min(t["scan"] for t in state["tabs"].values()) - margin)
        selected = [b for b in selected if (b["started"] or b["created"]) >= cutoff]
    chosen = {f"{b['job']}/{b['build']}" for b in selected}
    retry = [u["rec"] for k, u in state["unresolved"].items() if k not in chosen and k not in given_up]
    # Builds archived without a log are re-checked for a late log even after TestGrid drops them.
    retry += [u["rec"] for k, u in state["no_log"].items()
              if k not in chosen and k not in given_up and k not in state["unresolved"]]
    tab_errors = [r for r in reports if r.get("error")]

    if args.list_only:
        for b in sorted(selected + retry, key=lambda b: b["started"] or b["created"]):
            print(iso(b["started"] or b["created"]), b["job"], b["build"], ",".join(t["tab"] for t in b["tabs"]))
        print(f"{len(reports)} tabs read ({len(tab_errors)} failed), {len(selected)} red builds, {len(retry)} to retry")
        for r in tab_errors:
            print("ERROR", r["dashboard"], r["tab"], r["error"], file=sys.stderr)
        return 1 if tab_errors else 0

    os.makedirs(os.path.join(root, "runs"), exist_ok=True)
    outcomes, errors, downloaded, unresolved, no_log = {}, [], 0, {}, {}
    max_bytes = int(args.max_file_mb * (1 << 20))
    futures = {pool.submit(archive, b, root, args.gcs, run_id, run_start, args.max_job_hours, args.gzip, max_bytes): b
               for b in selected + retry}
    done, not_done = cf.wait(futures, timeout=max(0, deadline - time.monotonic()))
    if not_done:
        ctx.abandoned.set()
    for fut in futures:
        b = futures[fut]
        key = f"{b['job']}/{b['build']}"
        previous = state["unresolved"].get(key, {})
        try:
            if fut in not_done:
                if archived_intact(root, b["job"], b["build"], deep=True):
                    outcomes.setdefault("already-archived", []).append(key)
                    if key in state["no_log"]:
                        no_log[key] = state["no_log"][key]  # keep re-checking it for a late log
                    continue  # finished just as the run gave up, or was already on disk
                raise TimeoutError("run timeout reached before this build finished; retrying next run")
            outcome, meta = fut.result()
            outcomes.setdefault(outcome, []).append(key)
            if outcome in ("archived", "archived-without-log", "repaired", "log-recovered"):
                downloaded += sum(meta[k]["bytes"] for k in ("log", "podinfo") if meta.get(k))
            if outcome == "pending":
                unresolved[key] = {"rec": b, "first_seen": previous.get("first_seen", run_start), "last_error": None}
            elif not meta.get("log"):
                first = state["no_log"].get(key, {}).get("first_seen", run_start)
                if run_start - first <= GIVE_UP_DAYS * 86400:
                    no_log[key] = {"rec": b, "first_seen": first}
        except Exception as e:
            message = f"{type(e).__name__}: {e}"
            first = previous.get("first_seen", run_start)
            if run_start - first > GIVE_UP_DAYS * 86400:
                message = f"giving up after {GIVE_UP_DAYS} days: {message}"
                given_up[key] = {"rec": b, "first_seen": first, "last_error": message}
            else:
                unresolved[key] = {"rec": b, "first_seen": first, "last_error": message}
            errors.append({"build": key, "error": message})
    for names in outcomes.values():
        names.sort()
    errors.sort(key=lambda e: e["build"])

    current_run = {"run": run_id, "started": run_start, "since_hours": args.since_hours,
                   "max_job_hours": args.max_job_hours, "tabs": reports}
    missing = ([{"rec": u["rec"], "status": u["last_error"] or "waiting for GCS upload"} for u in unresolved.values()]
               + [{"rec": u["rec"], "status": f"given up: {u['last_error']}"} for u in given_up.values()])
    total, bad = write_index(root, time.time(), current_run, missing)
    errors += [{"build": None, "error": f"unreadable {b}"} for b in bad]
    elapsed = time.time() - run_start
    limit = ctx.limit
    concurrency = {"cpus": cpu_count(), "start": args.start_concurrency, "ceiling": limit.ceiling,
                   "peak": limit.peak, "final": int(limit.cap), "pushbacks": limit.pushbacks}
    write_json(os.path.join(root, "runs", f"{run_id}.json"), dict(
        current_run,
        finished=time.time(),
        red_builds_seen=len(builds),
        retried_from_state=len(retry),
        outcomes=outcomes,
        errors=errors,
        warnings=warnings,
        bytes_downloaded=downloaded,
        concurrency=concurrency,
    ))
    write_json(os.path.join(root, "state.json"),
               {"tabs": tab_scans, "unresolved": unresolved, "given_up": given_up, "no_log": no_log})

    counts = [f"{k}={len(v)}" for k, v in sorted(outcomes.items())] + [f"errors={len(errors)}"]
    print(f"run {run_id}: {len(reports)} tabs read ({len(tab_errors)} failed), {len(builds)} red builds on TestGrid, "
          f"{len(selected)} selected, {len(retry)} retried: {', '.join(counts)}; archive holds {total} builds")
    print(f"  {downloaded / 1e6:.1f} MB in {elapsed:.1f}s ({downloaded / 1e6 / max(elapsed, 1e-9):.1f} MB/s); "
          f"in-flight requests: start {concurrency['start']}, peak {concurrency['peak']}, "
          f"final {concurrency['final']} of max {concurrency['ceiling']}, {concurrency['pushbacks']} pushbacks")
    for r in tab_errors:
        print("ERROR tab", r["dashboard"], r["tab"], r["error"], file=sys.stderr)
    for e in errors:
        print("ERROR build", e["build"], e["error"], file=sys.stderr)
    for w in warnings:
        print("WARNING", w, file=sys.stderr)
    # A possible gap in TestGrid history or a lost retry list means builds may be
    # missing without an error naming them, so it fails the run too.
    serious = [w for w in warnings if "may be missing" in w or "state.json" in w]
    return 1 if tab_errors or errors or serious else 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    sys.stderr.flush()
    if context().abandoned.is_set():
        os._exit(code)  # do not wait for workers abandoned at the run timeout
    sys.exit(code)
