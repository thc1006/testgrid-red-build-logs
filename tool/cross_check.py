#!/usr/bin/env python3
"""Independently verify an archive written by fetch_red_logs.py.

For one run (default: the latest), within --hours before it started:
  integrity  every archived file (build-log.txt or podinfo.json) still has the
             md5 and length in its meta.json, and GCS still holds the content it
             was taken from; a build archived without a log still has none in GCS
  testgrid   every column TestGrid paints red now, whose build finished well
             before the run, is archived (red cells recomputed here)
  gcs        every build of the dashboards' jobs that failed is archived, or
             the reason it is not is shown (TestGrid does not paint it red,
             or does not list it)
A difference in GCS for a build whose copy is not settled yet (taken before
Prow finished uploading) is only noted: the next fetch compares it again.
Exits 1 when a check finds a problem.
"""
import argparse
import concurrent.futures as cf
import hashlib
import math
import os
import sys
import threading
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
from fetch_red_logs import GCS, PROW_EPOCH_MS, TESTGRID, NotFound, get_json, iso, request, table_url  # noqa: E402

# Recomputed here on purpose: TestGrid's UI maps these statuses to its red
# classes (fail, timeout, categorized-fail, tool), all #a00.
PAINTED_RED = {9, 10, 12, 14}
NAMES = {0: "NO_RESULT", 1: "PASS", 2: "PASS_WITH_ERRORS", 3: "PASS_WITH_SKIPS", 4: "RUNNING",
         5: "ABORTED", 6: "UNKNOWN", 7: "CANCEL", 8: "BLOCKED", 9: "TIMED_OUT", 10: "CATEGORIZED_FAIL",
         11: "BUILD_FAIL", 12: "FAIL", 13: "FLAKY", 14: "TOOL_FAIL", 15: "BUILD_PASSED"}


def columns(table):
    """Yield (build, start seconds, set of cell statuses) for each column.

    A column without a start time (timestamp 0), or with one more than a day
    before its build was made (TestGrid dates a column it could not read by an
    earlier one) or in the future, is dated by its build ID.
    Decoded here independently of the fetcher, and as strictly: a malformed
    row raises ValueError. Each status gets a difference array over the
    columns, so the work grows with the number of runs, not rows x columns.
    """
    builds, stamps = table["changelists"], table["timestamps"]
    width = len(builds)
    if not isinstance(builds, list) or not isinstance(stamps, list) or len(stamps) != width:
        raise ValueError("changelists and timestamps must be lists of the same length")
    edges = {}
    for row in table.get("tests", []):
        col = 0
        for run in row["statuses"]:
            value, count = run["value"], run["count"]
            if type(count) is not int or count <= 0:
                raise ValueError(f"row {str(row.get('name'))[:80]!r}: bad run length {count!r}")
            if type(value) is not int or value not in NAMES:
                raise ValueError(f"row {str(row.get('name'))[:80]!r}: bad status {value!r}")
            if col + count > width:
                raise ValueError(f"row {str(row.get('name'))[:80]!r} is wider than the table")
            diff = edges.setdefault(value, [0] * (width + 1))
            diff[col] += 1
            diff[col + count] -= 1
            col += count
        if col != width:
            raise ValueError(f"row {str(row.get('name'))[:80]!r} spans {col} of {width} columns")
    cells = [set() for _ in range(width)]
    for value, diff in edges.items():
        open_runs = 0
        for i in range(width):
            open_runs += diff[i]
            if open_runs:
                cells[i].add(value)
    latest = time.time() + 86400
    for i, build in enumerate(builds):
        start = stamps[i] // 1000 if stamps[i] > 0 else 0
        yield build, (start if created(build) - 86400 <= start <= latest else created(build)), cells[i]


class Unreachable:
    """A lookup that failed (not a 404): reported, never taken as an answer."""

    def __init__(self, error):
        self.error = f"{type(error).__name__}: {error}"


def try_json(url):
    """The JSON object at url: None on 404, MALFORMED if it is not a JSON object,
    Unreachable if it cannot be fetched."""
    try:
        found = get_json(url)
    except NotFound:
        return None
    except (frl.BadJSON, ValueError):
        return dict(MALFORMED)
    except Exception as e:
        return Unreachable(e)
    return found if isinstance(found, dict) else dict(MALFORMED)


MALFORMED = {"malformed": True}


def number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


INT64 = range(-(1 << 63), 1 << 63)
FINISHED_TYPES = {"timestamp": int, "passed": bool, "result": str}


def passed(finished):
    """Whether finished.json alone says TestGrid shows the build as passed: a finish
    time, and passed (no `passed`: SUCCESS). Like the fetch, this check then reads no
    other file, though TestGrid would paint it red for a malformed one."""
    if not finished or finished.get("malformed") is True or not decodable(finished, FINISHED_TYPES):
        return False
    stamp, ok = finished.get("timestamp"), finished.get("passed")
    return stamp is not None and stamp > 0 and (ok if isinstance(ok, bool) else finished.get("result") == "SUCCESS")


def decodable(doc, types):
    """Whether Go's JSON decoder, which TestGrid reads these files with, accepts the
    fields TestGrid uses: each absent, null, or of its Go type (an int fits int64)."""
    return all(doc.get(k) is None or (type(doc[k]) is t and (t is not int or doc[k] in INT64))
               for k, t in types.items())


def testgrid_red(finished, started, now):
    """TestGrid's own rule for the Overall cell (readResult in its
    pkg/updater/read.go; overallCell and deadline in pkg/updater/gcs.go), written
    here independently of the fetcher: a build with started.json is red when
    started.json or finished.json is malformed or has a field TestGrid cannot
    decode, when it finished without passing (no `passed`: its result is not
    SUCCESS), or when it has gone 24 hours without finishing."""
    if started is None:
        return False
    if started.get("malformed") is True or (finished or {}).get("malformed") is True:
        return True
    if not decodable(started, {"timestamp": int}) or (finished and not decodable(finished, FINISHED_TYPES)):
        return True
    stamp = (finished or {}).get("timestamp")
    if stamp is None:  # running, or finished without saying when: red once 24 hours have passed
        start = started.get("timestamp")  # (a start time it cannot date counts as none)
        return number(start) and 0 < start <= now + 86400 and now - start > 24 * 3600
    if stamp <= 0:
        return True  # TestGrid's deadline, an hour after this "finish", is long past
    return not passed(finished)


def list_builds(gcs, query, since):
    """Build IDs under gs://<query>/ created at or after `since` (seconds)."""
    bucket, _, prefix = query.partition("/")
    prefix += "/"
    first = max(0, since * 1000 - PROW_EPOCH_MS) << 22
    ids, token = [], None
    for _ in range(frl.MAX_LIST_PAGES):
        params = {"prefix": prefix, "delimiter": "/", "startOffset": f"{prefix}{first}",
                  "fields": "prefixes,nextPageToken"}
        if token:
            params["pageToken"] = token
        page = get_json(f"{gcs}/storage/v1/b/{bucket}/o?{urllib.parse.urlencode(params)}")
        if not isinstance(page, dict) or not isinstance(page.get("prefixes", []), list):
            raise ValueError(f"unexpected listing page for gs://{query}/")
        ids += [p[len(prefix):].rstrip("/") for p in page.get("prefixes", []) if isinstance(p, str) and p.startswith(prefix)]
        token = page.get("nextPageToken")
        if not token:
            # compare as numbers: older build IDs have fewer digits
            return [i for i in ids if i.isascii() and i.isdigit() and len(i) <= 20 and int(i) >= first]
    raise ValueError(f"the listing of gs://{query}/ did not end after {frl.MAX_LIST_PAGES} pages")


def created(build):
    return ((int(build) >> 22) + PROW_EPOCH_MS) // 1000


def non_negative(text):
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {text!r}") from None
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError(f"must be a finite number >= 0, got {text!r}")
    return value


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--archive", required=True)
    p.add_argument("--run", help="run to verify: its folder or run.json (default: the latest)")
    p.add_argument("--hours", type=frl.positive(float), default=48, help="window before the run to verify (default: 48)")
    p.add_argument("--lag-minutes", type=non_negative, default=60,
                   help="builds finishing this close to the run may not have been on TestGrid yet (default: 60)")
    p.add_argument("--max-concurrency", type=frl.positive(int), default=min(64, 16 * frl.cpu_count()))
    p.add_argument("--max-run-age-hours", type=frl.positive(float), default=2,
                   help="fail if the latest run report is older than this (default: 2)")
    p.add_argument("--timeout-minutes", type=frl.positive(float), default=15,
                   help="give up (and fail) if verification takes longer than this")
    p.add_argument("--testgrid", default=TESTGRID)
    p.add_argument("--gcs", default=GCS)
    args = p.parse_args(argv)
    # cross_check only reads, so a hard exit on timeout cannot leave anything half-written.
    watchdog = threading.Timer(args.timeout_minutes * 60, timed_out, args=(args.timeout_minutes,))
    watchdog.daemon = True
    watchdog.start()
    try:
        return verify(args)
    except KeyboardInterrupt:
        print("  PROBLEM: interrupted", flush=True)
        os._exit(130)  # read-only, so stop now instead of waiting for worker threads
    finally:
        watchdog.cancel()


def timed_out(minutes):
    print(f"  PROBLEM: verification did not finish within {minutes:g} minutes", flush=True)
    os._exit(1)


def run_start(path):
    """A run report's start time, None if it is unreadable or not a time it could have started at."""
    try:
        started = frl.read_json(path).get("started")
    except (OSError, ValueError, AttributeError):
        return None
    return started if frl.plausible_time(started, time.time()) else None


def latest_run(root):
    """Path of the run.json in the newest run folder that has a usable one, or None.
    Folders are named by when their run started, so a damaged report cannot pose as newer."""
    for rel in reversed(frl.run_dirs(root)):
        path = os.path.join(root, rel, "run.json")
        if run_start(path) is not None:
            return path
    return None


def local_digest(path):
    """(md5, length, size on disk) of an archived file; a .gz is gunzipped strictly."""
    md5, size = frl.file_digest(path)
    return md5, size, os.path.getsize(path)


class LocalDamage(str):
    """A problem with the archive's own copy: never excused by the build being unsettled."""


def remote_raw_md5(url):
    """md5 of what GCS serves, gunzipped if it is stored gzipped (bounded memory)."""
    def consume(r):
        enc = r.headers.get("Content-Encoding", "identity")
        if enc not in ("identity", "gzip"):
            raise frl.IntegrityError(f"unsupported Content-Encoding {enc!r}")
        md5, got = hashlib.md5(), 0
        data = frl.chunks(r, url)
        for piece in frl.gunzipped(data) if enc == "gzip" else data:
            md5.update(piece)
            got += len(piece)
        return md5.hexdigest()
    return request(url, consume=consume, headers={"Accept-Encoding": "gzip"})


def check_file(root, rel, m, key, default):
    """What is wrong with one archived file (build-log.txt or podinfo.json): a
    LocalDamage or a "GCS changed" message, or None."""
    info = m[key]
    name = default if info.get("omitted") else info.get("file", default)
    label = f"{m['job']}/{m['build']} {name}"
    if not info.get("omitted"):  # a file too large to store is only compared with GCS
        if name not in frl.LOG_FILES:
            return LocalDamage(f"{label}: unexpected file name in meta.json")
        md5, size, on_disk = local_digest(os.path.join(root, rel, name))
        if (md5, size) != (info["md5"], info["bytes"]):
            return LocalDamage(f"{label}: local md5 {md5} ({size} bytes) != meta {info['md5']} ({info['bytes']} bytes)")
        if on_disk != info.get("file_bytes", info["bytes"]):
            return LocalDamage(f"{label}: {on_disk} bytes on disk, meta says {info.get('file_bytes')}")
    url = m["log_url"].rsplit("/", 1)[0] + "/" + default
    try:
        headers = request(url, consume=lambda r: r.headers, method="HEAD", headers={"Accept-Encoding": "gzip"})
    except NotFound:
        return None  # GCS no longer has it; the archive holds the only copy
    now = frl.remote_identity(headers)
    if "gcs" in info or now["encoding"] == "identity":
        was = frl.recorded_identity(info)
        if not frl.same_object(was, now):
            return (f"{label}: GCS changed since it was archived (GCS md5 {now['md5']}, {now['bytes']} bytes, "
                    f"{now['encoding']}; archived from md5 {was.get('md5')}, {was.get('bytes')} bytes)")
    elif remote_raw_md5(url) != info["md5"]:
        # an old record of a gzip-stored object: compare the gunzipped content
        return f"{label}: GCS content changed since it was archived (gunzipped md5 differs)"
    return None


def verify(args):
    problems, notes = [], []
    try:
        return check(args, problems, notes)
    except Exception as e:  # keep what was already found
        problems.append(f"verification stopped: {type(e).__name__}: {e}")
        for n in notes:
            print("  note:", n)
        for pr in problems:
            print("  PROBLEM:", pr)
        print(f"{len(problems)} problem(s)")
        return 1


def check(args, problems, notes):
    root = args.archive
    if args.run:
        run_path = args.run if args.run.endswith(".json") else os.path.join(args.run, "run.json")
    else:
        run_path = latest_run(root)
    if run_path is None:
        print(f"  PROBLEM: no run report in {os.path.join(root, 'runs')}; the fetch never finished a run")
        return 1
    run = frl.read_json(run_path)
    t0 = run["started"]
    since = t0 - args.hours * 3600
    if run.get("since_hours") is not None:
        since = max(since, t0 - run["since_hours"] * 3600)
    settled = t0 - args.lag_minutes * 60
    tabs = [t for t in run["tabs"] if t.get("tab") is not None]
    problems += [f"tab not scanned by the run: {t['dashboard']}#{t['tab']}: {t['error']}" for t in run["tabs"] if t.get("error")]
    tabs = [t for t in tabs if not t.get("error")]
    problems += [f"the run reported an error for {e['build']}: {e['error']}" for e in run.get("errors", [])]
    # A gap in TestGrid history (or a lost retry list) means red builds may be gone
    # from TestGrid unseen, so the archive cannot be certified complete.
    serious = run.get("serious_warnings")
    if not isinstance(serious, list):  # a report from before the run listed them itself
        serious = [w for w in run.get("warnings", []) if any(k in w for k in frl.SERIOUS)]
    problems += [f"the run warned: {w}" for w in serious]
    default_settle = run.get("max_job_hours", 6) * 3600
    age = time.time() - run.get("finished", t0)
    if age > args.max_run_age_hours * 3600:
        problems.append(f"the latest run report ({run['run']}) is {age / 3600:.1f}h old; the last fetch did not finish")

    known, duplicates = frl.archived_builds(root)
    problems += [f"two copies of one build: {d}" for d in duplicates]
    if os.path.isdir(os.path.join(root, "logs")):
        problems.append("the archive still has an old-layout logs/ folder that the fetch could not move")
    # The archive cannot hold builds that scrolled off TestGrid before its first run
    # (the oldest run folder, as the fetch dates it).
    folders = frl.run_dirs(root)
    archive_start = frl.folder_time(folders[0].rsplit("/", 1)[1]) if folders else t0
    archived = {}
    for key, rel in known.items():
        path = os.path.join(root, rel, "meta.json")
        try:
            m = frl.read_json(path)
            if f"{m['job']}/{m['build']}" != key:
                raise ValueError(f"describes {m['job']}/{m['build']}")
            if not frl.valid_meta(m) or not frl.plausible_time(m["started"] or m["created"], time.time()):
                raise ValueError("not a usable archive meta.json")
            if not isinstance(m.get("log_url"), str):
                raise ValueError("no log_url")
            archived[(m["job"], m["build"])] = (rel, m)
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as e:
            problems.append(f"unreadable {path}: {type(e).__name__}: {e}")
    job_of = {t["query"]: t["query"].rsplit("/", 1)[-1] for t in tabs}
    print(f"verifying run {run['run']}: window {iso(since)} .. {iso(t0)}, {len(tabs)} tabs, {len(archived)} builds archived")

    ctx = frl.RunContext(frl.AdaptiveLimit(4 * frl.cpu_count(), args.max_concurrency))
    frl.bind_context(ctx)
    pool = cf.ThreadPoolExecutor(ctx.limit.ceiling, initializer=frl.bind_context, initargs=(ctx,))

    # 1. integrity of every archived file, and what GCS holds now
    def integrity(item):
        rel, m = item
        try:
            return check_build(rel, m)
        except Exception as e:
            return [LocalDamage(f"{m.get('job')}/{m.get('build')}: cannot verify: {type(e).__name__}: {e}")]

    def check_build(rel, m):
        found = [check_file(root, rel, m, key, default)
                 for key, default in (("log", "build-log.txt"), ("podinfo", "podinfo.json")) if m.get(key)]
        if not m.get("log"):
            try:  # archived without a log: has one appeared in GCS since?
                request(m["log_url"], consume=lambda r: r.headers, method="HEAD")
                found.append(f"{m['job']}/{m['build']}: GCS now has a build-log.txt that is not archived")
            except NotFound:
                pass
        return [p for p in found if p]

    recent = [(rel, m) for rel, m in archived.values() if (m["started"] or m["created"]) >= since]
    unsettled = [m for _, m in recent if m.get("settled") is False]
    bad = 0
    for (_, m), found in zip(recent, pool.map(integrity, recent), strict=True):
        bad += bool(found)
        for p in found:
            # GCS may still change a copy that is not settled, and the next fetch compares it again
            if not isinstance(p, LocalDamage) and m.get("settled") is False and m.get("final") is not True:
                notes.append(f"{p} (not settled yet: the next fetch compares it again)")
            else:
                problems.append(p)
    print(f"  integrity: {len(recent)} builds checked ({sum(1 for _, m in recent if m.get('log'))} with a log, "
          f"{sum(1 for _, m in recent if not m.get('log') and m.get('podinfo'))} with only podinfo.json), "
          f"{bad} differ, {len(unsettled)} not settled yet")

    # 2. every column TestGrid paints red now
    def load(t):
        try:
            return get_json(table_url(args.testgrid, t["dashboard"], t["tab"]))
        except Exception as e:
            problems.append(f"cannot re-read {t['dashboard']}#{t['tab']}: {type(e).__name__}: {e}")
            return None

    tables = dict(zip([(t["dashboard"], t["tab"]) for t in tabs], pool.map(load, tabs), strict=True))
    settle_of = {}  # per job, the longest margin of its tabs, as the fetch takes it
    for t in tabs:
        h = t.get("max_job_hours")
        margin = h * 3600 if number(h) and h > 0 else default_settle
        settle_of[t["query"]] = max(settle_of.get(t["query"], 0), margin)
    red_now, statuses, oldest_shown, undecoded, shown = {}, {}, {}, set(), set()
    for (dashboard, tab), table in tables.items():
        if table is None:
            continue
        try:
            job = table["query"].rsplit("/", 1)[-1]
            cols = list(columns(table))
        except (KeyError, TypeError, AttributeError, ValueError) as e:
            problems.append(f"cannot decode {dashboard}#{tab}: {type(e).__name__}: {e}")
            undecoded.add(table.get("query") if isinstance(table, dict) else None)
            continue
        for build, start, cells in cols:
            statuses.setdefault((job, build), set()).update(cells)
            if cells - {0, 6}:  # a column with results (not TestGrid's grey placeholder)
                shown.add((job, build))
                oldest_shown[table["query"]] = min(oldest_shown.get(table["query"], start), start)
            if start >= since and cells & PAINTED_RED:
                red_now.setdefault((job, build), (table["query"], [], start))[1].append(f"{dashboard}#{tab}")
    unarchived = [(k, v) for k, v in red_now.items() if k not in archived]
    finished = dict(zip([k for k, _ in unarchived],
                        pool.map(lambda kv: try_json(f"{args.gcs}/{kv[1][0]}/{kv[0][1]}/finished.json"), unarchived),
                        strict=True))
    late = 0
    for (job, build), (query, where, start) in unarchived:
        fin = finished[(job, build)]
        if isinstance(fin, Unreachable):
            problems.append(f"cannot check red build {job}/{build}: finished.json: {fin.error}")
            continue
        stamp = fin.get("timestamp") if fin else None
        stamp = stamp if isinstance(stamp, (int, float)) and not isinstance(stamp, bool) else None
        if stamp is not None:
            # finished.json says when it finished: that decides
            if stamp <= settled:
                problems.append(f"MISSED red build {job}/{build} ({', '.join(where)}), finished {iso(stamp)}")
            else:
                late += 1
        elif start < settled - 2 * settle_of.get(query, default_settle):
            # long enough ago that the fetch records it even without anything in GCS
            problems.append(f"MISSED red build {job}/{build} ({', '.join(where)}), started {iso(start)}, "
                            f"finished.json {'present' if fin is not None else 'missing'}")
        else:
            late += 1
    print(f"  testgrid: {len(red_now)} red builds in window, {len(red_now) - len(unarchived)} archived, "
          f"{late} not settled at run time (finished <{args.lag_minutes:.0f} min before it, or no finished.json), "
          f"{len(unarchived) - late} missed")

    # 3. every failed build in GCS
    queries = sorted({t["query"] for t in tabs})

    def listing(q):
        try:
            return list_builds(args.gcs, q, int(since))
        except Exception as e:
            problems.append(f"cannot list gs://{q}/: {type(e).__name__}: {e}")
            return []
    listed = dict(zip(queries, pool.map(listing, queries), strict=True))
    candidates = [(q, b) for q, ids in listed.items() for b in ids if created(b) >= since]
    results = dict(zip(candidates, pool.map(lambda qb: try_json(f"{args.gcs}/{qb[0]}/{qb[1]}/finished.json"), candidates),
                       strict=True))
    # A build TestGrid no longer shows (it scrolled off: it is older than the
    # oldest column with results) but painted red by its own rule should have
    # been archived, or taken from GCS for a history gap, if the archive ran then.
    unread = {t["query"] for t in tabs if tables.get((t["dashboard"], t["tab"])) is None} | undecoded
    unlisted = [(q, b) for (q, b), fin in results.items()
                if not isinstance(fin, Unreachable) and not passed(fin)
                and not (fin and number(fin.get("timestamp")) and fin["timestamp"] > settled)
                and (job_of[q], b) not in archived and (job_of[q], b) not in shown
                and created(b) >= archive_start and q not in unread and q in oldest_shown and created(b) < oldest_shown[q]]
    started = dict(zip(unlisted, pool.map(lambda qb: try_json(f"{args.gcs}/{qb[0]}/{qb[1]}/started.json"), unlisted),
                       strict=True))
    for (q, b) in unlisted:
        st = started[(q, b)]
        if isinstance(st, Unreachable):
            problems.append(f"cannot check {job_of[q]}/{b}: started.json: {st.error}")
        elif testgrid_red(results[(q, b)], st, t0):
            problems.append(f"MISSED red build {job_of[q]}/{b}: TestGrid no longer lists it, but by TestGrid's rule "
                            f"it was red, and it is not archived")
    counts = {}
    for (q, b), fin in results.items():
        key = (job_of[q], b)
        if isinstance(fin, Unreachable):
            counts["unreadable finished.json"] = counts.get("unreadable finished.json", 0) + 1
            notes.append(f"{key[0]}/{b}: cannot read finished.json: {fin.error}")
            continue
        if fin is None:
            counts["unfinished"] = counts.get("unfinished", 0) + 1
            continue
        stamp = fin.get("timestamp", 0)
        if fin.get("malformed") is True:
            result = "MALFORMED"  # not JSON TestGrid can read: red to it
        elif not isinstance(stamp, (int, float)) or isinstance(stamp, bool) or stamp > settled:
            counts["finished too close to the run"] = counts.get("finished too close to the run", 0) + 1
            continue
        else:
            result = str(fin.get("result") or ("SUCCESS" if fin.get("passed") else "FAILURE")).upper()
        counts[result] = counts.get(result, 0) + 1
        if result == "SUCCESS":
            if key in archived:
                notes.append(f"{key[0]}/{b}: red on TestGrid but Prow says SUCCESS (archived)")
            continue
        if key in archived:
            continue
        cells = statuses.get(key)
        if (q, b) in started:
            continue  # no longer shown (at most in a placeholder column): judged above
        if cells is None:
            notes.append(f"{key[0]}/{b}: {result}, not listed on TestGrid (not archived)")
        elif cells & PAINTED_RED:
            problems.append(f"MISSED failed build {key[0]}/{b}: {result} and painted red on TestGrid")
        else:
            notes.append(f"{key[0]}/{b}: {result}, TestGrid paints it {'/'.join(sorted(NAMES[c] for c in cells))} "
                         f"(not red, not archived)")
    pool.shutdown()
    print(f"  gcs: {len(candidates)} builds created in window: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    for n in notes:
        print("  note:", n)
    for pr in problems:
        print("  PROBLEM:", pr)
    print("OK" if not problems else f"{len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
