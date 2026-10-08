#!/usr/bin/env python3
"""Independently verify an archive written by fetch_red_logs.py.

For one run (default: the latest), within --hours before it started:
  integrity  every archived log still has the md5 in its meta.json, and GCS
             still stores that md5
  testgrid   every column TestGrid paints red now, whose build finished well
             before the run, is archived (red cells recomputed here)
  gcs        every build of the dashboards' jobs that failed is archived, or
             the reason it is not is shown (TestGrid does not paint it red,
             or does not list it)
Exits 1 when a check finds a problem.
"""
import argparse
import concurrent.futures as cf
import glob
import gzip
import hashlib
import json
import os
import sys
import threading
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
from fetch_red_logs import GCS, PROW_EPOCH_MS, TESTGRID, NotFound, gcs_md5, get_json, iso, request, table_url  # noqa: E402

# Recomputed here on purpose: TestGrid's UI maps these statuses to its red
# classes (fail, timeout, categorized-fail, tool), all #a00.
PAINTED_RED = {9, 10, 12, 14}
NAMES = {0: "NO_RESULT", 1: "PASS", 2: "PASS_WITH_ERRORS", 3: "PASS_WITH_SKIPS", 4: "RUNNING",
         5: "ABORTED", 6: "UNKNOWN", 7: "CANCEL", 8: "BLOCKED", 9: "TIMED_OUT", 10: "CATEGORIZED_FAIL",
         11: "BUILD_FAIL", 12: "FAIL", 13: "FLAKY", 14: "TOOL_FAIL", 15: "BUILD_PASSED"}


def columns(table):
    """Yield (build, start seconds, set of cell statuses) for each column.

    A column without a start time (timestamp 0) is dated by its build ID.
    """
    cells = [[r["value"] for r in t["statuses"] for _ in range(r["count"])] for t in table.get("tests", [])]
    for i, build in enumerate(table["changelists"]):
        stamp = table["timestamps"][i]
        yield build, (stamp // 1000 if stamp > 0 else created(build)), {row[i] for row in cells}


def try_json(url):
    try:
        return get_json(url)
    except NotFound:
        return None


def list_builds(gcs, query, since):
    """Build IDs under gs://<query>/ created at or after `since` (seconds)."""
    bucket, _, prefix = query.partition("/")
    prefix += "/"
    first = (since * 1000 - PROW_EPOCH_MS) << 22
    ids, token = [], None
    while True:
        params = {"prefix": prefix, "delimiter": "/", "startOffset": f"{prefix}{first}",
                  "fields": "prefixes,nextPageToken"}
        if token:
            params["pageToken"] = token
        page = get_json(f"{gcs}/storage/v1/b/{bucket}/o?{urllib.parse.urlencode(params)}")
        ids += [p[len(prefix):].rstrip("/") for p in page.get("prefixes", [])]
        token = page.get("nextPageToken")
        if not token:
            return [i for i in ids if i.isdigit() and len(i) == len(str(first))]


def created(build):
    return ((int(build) >> 22) + PROW_EPOCH_MS) // 1000


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--archive", required=True)
    p.add_argument("--run", help="run report to verify (default: the latest)")
    p.add_argument("--hours", type=float, default=48, help="window before the run to verify (default: 48)")
    p.add_argument("--lag-minutes", type=float, default=60,
                   help="builds finishing this close to the run may not have been on TestGrid yet (default: 60)")
    p.add_argument("--max-concurrency", type=int, default=min(64, 16 * frl.cpu_count()))
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
    finally:
        watchdog.cancel()


def timed_out(minutes):
    print(f"  PROBLEM: verification did not finish within {minutes:g} minutes", flush=True)
    os._exit(1)


def verify(args):

    run_path = args.run or sorted(glob.glob(os.path.join(args.archive, "runs", "*.json")))[-1]
    with open(run_path) as f:
        run = json.load(f)
    t0 = run["started"]
    since = t0 - args.hours * 3600
    if run.get("since_hours") is not None:
        since = max(since, t0 - run["since_hours"] * 3600)
    settled = t0 - args.lag_minutes * 60
    problems, notes = [], []
    tabs = [t for t in run["tabs"] if t.get("tab")]
    problems += [f"tab not scanned by the run: {t['dashboard']}#{t['tab']}: {t['error']}" for t in run["tabs"] if t.get("error")]
    tabs = [t for t in tabs if not t.get("error")]
    problems += [f"the run reported an error for {e['build']}: {e['error']}" for e in run.get("errors", [])]
    # A gap in TestGrid history (or a lost retry list) means red builds may be gone
    # from TestGrid unseen, so the archive cannot be certified complete.
    problems += [f"the run warned: {w}" for w in run.get("warnings", []) if "may be missing" in w or "state.json" in w]
    settle = run.get("max_job_hours", 6) * 3600
    age = time.time() - run.get("finished", t0)
    if age > args.max_run_age_hours * 3600:
        problems.append(f"the latest run report ({run['run']}) is {age / 3600:.1f}h old; the last fetch did not finish")

    archived = {}
    for path in glob.glob(os.path.join(args.archive, "logs", "*", "*", "meta.json")):
        try:
            with open(path) as f:
                m = json.load(f)
            archived[(m["job"], m["build"])] = m
        except (OSError, ValueError, KeyError, TypeError) as e:
            problems.append(f"unreadable {path}: {type(e).__name__}: {e}")
    job_of = {t["query"]: t["query"].rsplit("/", 1)[-1] for t in tabs}
    print(f"verifying run {run['run']}: window {iso(since)} .. {iso(t0)}, {len(tabs)} tabs, {len(archived)} builds archived")

    ctx = frl.RunContext(frl.AdaptiveLimit(4 * frl.cpu_count(), args.max_concurrency))
    frl.bind_context(ctx)
    pool = cf.ThreadPoolExecutor(ctx.limit.ceiling, initializer=frl.bind_context, initargs=(ctx,))

    # 1. integrity
    def integrity(m):
        try:
            return check_one(m)
        except Exception as e:
            return f"{m.get('job')}/{m.get('build')}: cannot verify: {type(e).__name__}: {e}"

    def check_one(m):
        log = m["log"]
        if not log:
            # archived without a log: has one appeared in GCS since?
            try:
                request(m["log_url"], consume=lambda r: r.headers, method="HEAD")
            except NotFound:
                return None
            return f"{m['job']}/{m['build']}: GCS now has a build-log.txt that is not archived"
        if not log.get("omitted"):  # a file too large to store is only compared with GCS
            path = os.path.join(args.archive, "logs", m["job"], m["build"], log.get("file", "build-log.txt"))
            md5 = hashlib.md5()
            with (gzip.open if path.endswith(".gz") else open)(path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    md5.update(chunk)
            if md5.hexdigest() != log["md5"]:
                return f"{m['job']}/{m['build']}: local md5 {md5.hexdigest()} != meta {log['md5']}"
        headers = request(m["log_url"], consume=lambda r: r.headers, method="HEAD")
        if headers.get("x-goog-stored-content-encoding", "identity") == "identity" and gcs_md5(headers) != log["md5"]:
            return f"{m['job']}/{m['build']}: GCS md5 {gcs_md5(headers)} != archived {log['md5']}"
        return None

    recent = [m for m in archived.values() if (m["started"] or m["created"]) >= since]
    bad = [r for r in pool.map(integrity, recent) if r]
    problems += bad
    print(f"  integrity: {len(recent)} builds checked ({sum(1 for m in recent if m['log'])} with a log), {len(bad)} bad")

    # 2. every column TestGrid paints red now
    def load(t):
        try:
            return get_json(table_url(args.testgrid, t["dashboard"], t["tab"]))
        except Exception as e:
            problems.append(f"cannot re-read {t['dashboard']}#{t['tab']}: {type(e).__name__}: {e}")
            return None

    tables = dict(zip([(t["dashboard"], t["tab"]) for t in tabs], pool.map(load, tabs), strict=True))
    red_now, statuses = {}, {}
    for (dashboard, tab), table in tables.items():
        if table is None:
            continue
        job = table["query"].rsplit("/", 1)[-1]
        for build, start, cells in columns(table):
            statuses.setdefault((job, build), set()).update(cells)
            if start >= since and cells & PAINTED_RED:
                red_now.setdefault((job, build), (table["query"], [], start))[1].append(f"{dashboard}#{tab}")
    unarchived = [(k, v) for k, v in red_now.items() if k not in archived]
    finished = dict(zip([k for k, _ in unarchived],
                        pool.map(lambda kv: try_json(f"{args.gcs}/{kv[1][0]}/{kv[0][1]}/finished.json"), unarchived),
                        strict=True))
    late = 0
    for (job, build), (_, where, start) in unarchived:
        fin = finished[(job, build)]
        if fin and fin.get("timestamp", 0) <= settled:
            problems.append(f"MISSED red build {job}/{build} ({', '.join(where)}), finished {iso(fin['timestamp'])}")
        elif start < settled - settle:
            # long enough ago that it must have finished, whatever GCS says
            problems.append(f"MISSED red build {job}/{build} ({', '.join(where)}), started {iso(start)}, "
                            f"finished.json {'present' if fin else 'missing'}")
        else:
            late += 1
    print(f"  testgrid: {len(red_now)} red builds in window, {len(red_now) - len(unarchived)} archived, "
          f"{late} not settled at run time (finished <{args.lag_minutes:.0f} min before it, or no finished.json), "
          f"{len(unarchived) - late} missed")

    # 3. every failed build in GCS
    queries = sorted({t["query"] for t in tabs})
    listed = dict(zip(queries, pool.map(lambda q: list_builds(args.gcs, q, int(since)), queries), strict=True))
    candidates = [(q, b) for q, ids in listed.items() for b in ids if created(b) >= since]
    results = dict(zip(candidates, pool.map(lambda qb: try_json(f"{args.gcs}/{qb[0]}/{qb[1]}/finished.json"), candidates),
                       strict=True))
    counts = {}
    for (q, b), fin in results.items():
        key = (job_of[q], b)
        if fin is None:
            counts["unfinished"] = counts.get("unfinished", 0) + 1
            continue
        if fin.get("timestamp", 0) > settled:
            counts["finished too close to the run"] = counts.get("finished too close to the run", 0) + 1
            continue
        result = str(fin.get("result") or ("SUCCESS" if fin.get("passed") else "FAILURE")).upper()
        counts[result] = counts.get(result, 0) + 1
        if result == "SUCCESS":
            if key in archived:
                notes.append(f"{key[0]}/{b}: red on TestGrid but Prow says SUCCESS (archived)")
            continue
        if key in archived:
            continue
        cells = statuses.get(key)
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
