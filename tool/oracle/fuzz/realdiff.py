"""Real builds of the archive's jobs, judged by TestGrid's own code and by the fetch:
realdiff.py ARCHIVE BUILDS_PER_JOB (the jobs are the tabs of the archive's latest run).

TestGrid's code is given each build as read here, independently of the fetch (a
plain paged listing of the build's folder, objects named as listed, a gzip-stored
one of unknown size, the junit files read decompressed, as TestGrid's GCS client
reads them). The fetch's side is fetch_red_logs.gcs_column itself: its listing, its
pages, the junit files it picks and reads, their sizes and encodings, made again at
the moment TestGrid's code judged the build. A mistake in either reading shows."""
import concurrent.futures as cf
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness  # noqa: E402  (puts tool/ on the path)
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import testgrid_rule as tr  # noqa: E402

GCS = frl.GCS


def get(url):
    """An object's bytes (served decompressed: no Accept-Encoding), None when there is none."""
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code < 500 or attempt == 3:
                raise
        except OSError:
            if attempt == 3:
                raise
        time.sleep(2 ** attempt)


def listing(bucket, prefix):
    items, token = [], None
    while True:
        q = {"prefix": prefix, "fields": "items(name,size,contentEncoding),nextPageToken"}
        if token:
            q["pageToken"] = token
        page = json.loads(get(f"{GCS}/storage/v1/b/{bucket}/o?" + urllib.parse.urlencode(q)))
        items += page.get("items", [])
        token = page.get("nextPageToken")
        if not token:
            return items


def oracle_case(query, build):
    """The build as TestGrid's code is given it, read here."""
    bucket, _, path = query.partition("/")
    folder = f"{path}/{build}/"
    files = {n: get(f"{GCS}/{query}/{build}/{n}.json") for n in ("started", "finished", "podinfo")}
    items = listing(bucket, folder)
    listed = {i["name"]: i for i in items}
    opened = {target for _, target in tr.junit_reads(sorted(listed))}
    artifacts = []
    for name, item in sorted(listed.items()):
        rel = name[len(folder):]
        if rel in ("started.json", "finished.json", "podinfo.json"):
            continue
        size = -1 if (item.get("contentEncoding") or "").lower() == "gzip" else int(item["size"])
        # TestGrid's code reads a junit file it opens unless it is too large (others: names only)
        data = get(f"{GCS}/{bucket}/{urllib.parse.quote(name, safe='/')}") \
            if name in opened and size <= tr.JUNIT_MAX_SIZE else None
        artifacts.append({"name": rel, "size": size, "data": data})
    return {"id": f"{query}/{build}", "stop": frl.created(build) - tr.DAYS_OF_RESULTS * 86400, **files,
            "artifacts": artifacts}


fetch_side = threading.local()  # the rule's arguments gcs_column assembled, on this thread
real_column = tr.column


def column(*args, **kwargs):
    fetch_side.args = (args, kwargs)
    return real_column(*args, **kwargs)


tr.column = column


def settled(query, build, now):
    """Whether nothing changes between the two readings of a build: it finished more than an
    hour ago (so its podinfo.json is in), or, without finished.json, it started more than 25
    hours ago (TestGrid then calls it failed)."""
    def stamp(body):
        try:
            value = json.loads(body).get("timestamp")
        except (TypeError, ValueError, AttributeError):
            return None
        return value if isinstance(value, int) and not isinstance(value, bool) else None
    finished_at = stamp(get(f"{GCS}/{query}/{build}/finished.json"))
    if finished_at is not None:
        return 0 < finished_at < now - 3600
    started = stamp(get(f"{GCS}/{query}/{build}/started.json"))  # none, or one without a usable time
    return started is not None and 0 < started < now - 25 * 3600


def judge(query, build, now):
    """(the build for TestGrid's code, the fetch's arguments to the rule), None for a build not
    settled yet, or the reason it cannot be compared."""
    try:
        if not settled(query, build, now):
            return None
        case = oracle_case(query, build)
        fetch_side.args = None
        col = frl.gcs_column(GCS, query, build, tr.DAYS_OF_RESULTS, now)
        if col.partial:
            return f"{query}/{build}: {col.partial}"
        return case, fetch_side.args
    except Exception as e:  # noqa: BLE001
        return f"{query}/{build}: {type(e).__name__}: {e}"


archive, per_job = sys.argv[1], int(sys.argv[2])
run = cc.latest_run(archive)
if run is None:
    print("No run report in the archive to take the jobs from.")
    sys.exit(0)
queries = sorted({t["query"] for t in frl.read_json(run)["tabs"] if t.get("query")})
now = int(time.time())
jobs = []
for q in queries:  # the newest builds, a few more than needed: some are still running
    builds = frl.list_build_ids(GCS, q, now - 7 * 86400, now - 3600)
    jobs += [(q, b) for b in sorted(builds, key=int)[-3 * per_job:]]
with cf.ThreadPoolExecutor(16, initializer=frl.bind_context, initargs=(frl.RunContext(frl.AdaptiveLimit(16, 16)),)) as pool:
    results = list(pool.map(lambda qb: judge(*qb, now), jobs))
kept, failed, judged = {}, [], []
for (q, _), r in sorted(zip(jobs, results, strict=True), key=lambda x: -int(x[0][1])):  # newest first
    if r is None or kept.get(q, 0) >= per_job:
        continue
    kept[q] = kept.get(q, 0) + 1
    (failed if isinstance(r, str) else judged).append(r)
print(len(queries), "jobs,", len(failed) + len(judged), "settled builds")
got = harness.run_oracle([case for case, _ in judged])
bad, reds = [], 0
for (case, (args, kwargs)), g in zip(judged, got, strict=True):
    if g is None:
        continue
    col = real_column("job", *args[1:6], g["now"], **kwargs)  # TestGrid's row names, at the moment it judged
    problems = harness.compare(case, g, col)
    reds += bool(col.errors and "TOOL_FAIL" in col.results or col.red_rows())
    if problems:
        bad.append((case["id"], problems))
for name, problems in bad[:20]:
    print("MISMATCH", name, problems)
for error in failed[:5]:
    print("UNREADABLE", error)
print(f"{len(judged) - len(bad)} of {len(judged)} real builds match ({len(failed)} could not be read); "
      f"{reds} red by the rule; {sum(len(c['artifacts']) for c, _ in judged)} objects listed")
sys.exit(1 if bad or failed else 0)
